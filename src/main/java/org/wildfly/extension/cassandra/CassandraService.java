/*
 * JBoss, Home of Professional Open Source
 *  Copyright ${year}, Red Hat, Inc., and individual contributors
 *  by the @authors tag. See the copyright.txt in the distribution for a
 *  full listing of individual contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wildfly.extension.cassandra;

import static java.io.File.separatorChar;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.CassandraDaemon;
import org.jboss.as.controller.services.path.AbsolutePathService;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.msc.inject.Injector;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

/**
 * The cassandra runtime service. Delegates to an adapter {@link CassandraDaemon} that wraps the actual C* services.
 *
 * @author Heiko Braun
 */
public class CassandraService implements Service<CassandraDaemon> {

    public static final String CASSANDRA_DATA_FILE_DIR = "cassandra/data";
    public static final String CASSANDRA_SAVED_CACHES_DIR = "cassandra/saved_caches";
    public static final String CASSANDRA_COMMIT_LOG_DIR = "cassandra/commitlog";

    public static final ServiceName BASE_SERVICE_NAME = ServiceName.JBOSS.append("cassandra");

    private final String clusterName;
    private final Config serviceConfig;

    private CassandraDaemon cassandraDaemon;
    private final InjectedValue<PathManager> pathManagerValue = new InjectedValue<PathManager>();

    public CassandraService(String clusterName, Config serviceConfig) {
        this.clusterName = clusterName;
        this.serviceConfig = serviceConfig;
    }

    public ServiceName getServiceName() {
        return BASE_SERVICE_NAME.append(clusterName);
    }

    @Override
    public CassandraDaemon getValue() throws IllegalStateException, IllegalArgumentException {
        return cassandraDaemon;
    }

    @Override
    public void start(StartContext context) throws StartException {

        try {
            CassandraLogger.LOGGER.infof("Starting embedded cassandra service '%s'", clusterName);

            // resolve the path location
            // includes the _clusterName_ suffix to avid conflicts when different configurations are started on the same base system
            PathManager pathManager = pathManagerValue.getValue();
            if (null == serviceConfig.data_file_directories) {
                serviceConfig.data_file_directories = new String[]{
                    resolve(pathManager, CASSANDRA_DATA_FILE_DIR, ServerEnvironment.SERVER_DATA_DIR) + separatorChar + clusterName};
            }

            if (null == serviceConfig.saved_caches_directory) {
                serviceConfig.saved_caches_directory = resolve(pathManager, CASSANDRA_SAVED_CACHES_DIR, ServerEnvironment.SERVER_DATA_DIR)
                        + separatorChar + clusterName;
            }

            if (null == serviceConfig.commitlog_directory) {
                serviceConfig.commitlog_directory = resolve(pathManager, CASSANDRA_COMMIT_LOG_DIR, ServerEnvironment.SERVER_DATA_DIR)
                        + separatorChar + clusterName;
            }
            boolean needConfigReload = DMRConfigLoader.CASSANDRA_CONFIG != null;
            // static injection needed due to the way C* initialises it's ConfigLoader
            DMRConfigLoader.CASSANDRA_CONFIG = serviceConfig;

            System.setProperty("cassandra.config.loader", DMRConfigLoader.class.getName());
            if(needConfigReload) {
                reloadConfig();
            }
            cassandraDaemon = new CassandraDaemon(true);
            cassandraDaemon.activate();
        } catch (Throwable e) {
            context.failed(new StartException(e));
        }
    }

    private void reloadConfig() throws IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchMethodException, InvocationTargetException {
        Method applyConfig = DatabaseDescriptor.class.getDeclaredMethod("applyConfig", Config.class);
        applyConfig.setAccessible(true);
        applyConfig.invoke(null, serviceConfig);
//        DatabaseDescriptor.applyConfig(serviceConfig); we need to change this in C* code
    }

    @Override
    public void stop(StopContext context) {
        if (cassandraDaemon != null) {
            CassandraLogger.LOGGER.infof("Stopping cassandra service '%s'.", clusterName);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            try {
                Set<ObjectInstance> result = mbs.queryMBeans(new ObjectName("org.apache.cassandra.*:*"), null);
                for(ObjectInstance cassandraMBean : result) {
                    try {
                        mbs.unregisterMBean(cassandraMBean.getObjectName());
                    } catch (InstanceNotFoundException | MBeanRegistrationException ex) {
                        //Who cares to unregister if it's not there.
                    }
                }
            } catch (MalformedObjectNameException ex) {
                CassandraLogger.LOGGER.warnf(ex, "Stopping cassandra service '%s', we have some error.", clusterName);
            }
            cassandraDaemon.deactivate();
            Schema.instance.clear();
            cassandraDaemon = null;
        }
    }

    public Injector<PathManager> getPathManagerInjector() {
        return pathManagerValue;
    }

    private String resolve(PathManager pathManager, String path, String relativeToPath) {
        // discard the relativeToPath if the path is absolute and must not be resolved according
        // to the default relativeToPath value
        String relativeTo = AbsolutePathService.isAbsoluteUnixOrWindowsPath(path) ? null : relativeToPath;
        return pathManager.resolveRelativePathEntry(path, relativeTo);
    }

}
