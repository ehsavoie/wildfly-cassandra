/*
 * Copyright (C) 2016 Red Hat, inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package org.wildfly.extension.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.apache.cassandra.service.CassandraDaemon;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.wildfly.extension.cassandra.logging.CassandraLogger;

/**
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class ClusterService implements Service<Cluster>{
    private Cluster cluster;
    private final String node;
    private final int port;
    private final boolean debug;
    private final InjectedValue<CassandraDaemon> daemonValue = new InjectedValue();
    public static final ServiceName BASE_SERVICE_NAME = ServiceName.JBOSS.append("cassandra-cluster");

    public ClusterService(String node, int port, boolean debug) {
        this.node = node;
        this.port = port;
        this.debug = debug;
    }

    public ServiceName getServiceName() {
        return BASE_SERVICE_NAME.append(node);
    }

    @Override
    public void start(StartContext context) throws StartException {
        cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        if (debug) {
            Metadata metadata = cluster.getMetadata();
            CassandraLogger.LOGGER.infof("Connected to cluster: %s\n", metadata.getClusterName());
            for (Host host : metadata.getAllHosts()) {
                CassandraLogger.LOGGER.infof("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
            }
        }
    }

    @Override
    public void stop(StopContext context) {
        cluster.close();
    }

    @Override
    public Cluster getValue() throws IllegalStateException, IllegalArgumentException {
        return cluster;
    }

    public Session getSession() {
        return cluster.connect();
    }

    public InjectedValue<CassandraDaemon> getDaemonValue() {
        return daemonValue;
    }
}
