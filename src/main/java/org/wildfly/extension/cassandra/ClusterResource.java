/*
 * Copyright (C) 2015 Red Hat, inc., and individual contributors
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

import org.wildfly.extension.cassandra.logging.CassandraLogger;

import static org.wildfly.extension.cassandra.CassandraModel.KEYSPACE;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.jboss.as.controller.ControlledProcessState;
import org.jboss.as.controller.ControlledProcessStateService;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.registry.DelegatingResource;
import org.jboss.as.controller.registry.PlaceholderResource;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;

/**
 * Custom resource to be able to display keyspaces.
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class ClusterResource extends DelegatingResource {

    private CassandraConnectionService clusterService;
    private ControlledProcessStateService processState;

    private ClusterResource(Resource delegate, ControlledProcessStateService processState, CassandraConnectionService service) {
        super(delegate);
        this.clusterService = service;
        this.processState = processState;
    }

    public ClusterResource() {
        this((Resource.Factory.create(false)), null, null);
    }

    void setService(CassandraConnectionService service) {
        this.clusterService = service;
    }
    
    void setDebugMode(boolean traceQuery) {
        this.clusterService.setDebug(traceQuery);
    }

    public void setProcessState(ControlledProcessStateService processState) {
        this.processState = processState;
    }

    @Override
    public Set<String> getChildrenNames(String childType) {
        if (KEYSPACE.equals(childType)) {
            return listKeyspaces().keySet();
        }
        return super.getChildrenNames(childType);
    }

    @Override
    public Resource requireChild(PathElement element) {
        if (KEYSPACE.equals(element.getKey())) {
            return listKeyspaces().get(element.getValue());
        }
        return super.requireChild(element);
    }

    @Override
    public boolean hasChildren(String childType) {
        if (KEYSPACE.equals(childType)) {
            return !listKeyspaces().isEmpty();
        }
        return super.hasChildren(childType);
    }

    @Override
    public boolean hasChild(PathElement element) {
        if (KEYSPACE.equals(element.getKey())) {
            return listKeyspaces().containsKey(element.getValue());
        }
        return super.hasChild(element);
    }

    @Override
    public Set<ResourceEntry> getChildren(String childType) {
        if (KEYSPACE.equals(childType)) {
            return new HashSet<>(listKeyspaces().values());
        }
        return super.getChildren(childType);
    }

    @Override
    public Resource navigate(PathAddress address) {
        if (address.size() > 0 && KEYSPACE.equals(address.getElement(0).getKey())) {
            if (address.size() > 1) {
                throw new NoSuchResourceException(address.getElement(1));
            }
            if (listKeyspaces().containsKey(address.getElement(0).getValue())) {
                return Resource.Factory.create(true);
            }
            throw new NoSuchResourceException(address.getElement(0));
        }
        return super.navigate(address);
    }

    @Override
    public Set<String> getChildTypes() {
        Set<String> types = super.getChildTypes();
        types.add(KEYSPACE);
        return types;
    }

    @Override
    public Resource getChild(PathElement element) {
        if (KEYSPACE.equals(element.getKey())) {
            Map<String, ResourceEntry> keyspaces = listKeyspaces();
            if (keyspaces.containsKey(element.getValue())) {
                return keyspaces.get(element.getValue());
            }
            return PlaceholderResource.INSTANCE;
        }
        return super.getChild(element);
    }

    public Session getSession() {
        return clusterService.getValue();
    }

    public void traceQuery(String query) {
        this.clusterService.traceQuery(query);
    }

    private Map<String, ResourceEntry> listKeyspaces() {
        Map<String, ResourceEntry> result = new HashMap<>();
        if (canAccessCluster()) {
            try (Session session = getSession()) {
                traceQuery("SELECT * FROM system_schema.keyspaces;");
                ResultSet rs = session.execute("SELECT * FROM system_schema.keyspaces;");
                for (Row row : rs) {
                    String name = row.getString("keyspace_name");
                    Map<String, String> options = row.getMap("replication", String.class, String.class);
                    int replication_factor = 1;
                    if (options.containsKey("replication_factor")) {
                        replication_factor = Integer.parseInt(options.get("replication_factor"));
                    }
                    String strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
                    if (options.containsKey("class")) {
                        strategy_class = options.get("class");
                    }
                    result.put(name, new KeyspaceResourceEntry(name, strategy_class, replication_factor));
                }
            } catch (NoHostAvailableException ex) {
                CassandraLogger.LOGGER.debug("Surprise Surprise ", ex);
            } catch (Exception ex) {
                CassandraLogger.LOGGER.error("Surprise ", ex);
            }
        }
        return result;
    }

    @SuppressWarnings("deprecation")
    private boolean canAccessCluster() {
        return processState != null && (processState.getCurrentState() == ControlledProcessState.State.RUNNING
                || processState.getCurrentState() == ControlledProcessState.State.RELOAD_REQUIRED
                || processState.getCurrentState() == ControlledProcessState.State.RESTART_REQUIRED);
    }

    public ControlledProcessStateService getProcessState() {
        return processState;
    }

    @Override
    public Resource clone() {
        return new ClusterResource(super.clone(), processState, clusterService);
    }

    public static class KeyspaceResourceEntry implements ResourceEntry {

        private final String name;
        private final PathElement path;
        private ModelNode model = new ModelNode();

        public KeyspaceResourceEntry(String name, String strategy_class, int replication_factor) {
            this.name = name;
            this.path = PathElement.pathElement(KEYSPACE, name);
            model.get(KeyspaceDefinition.CLASS.getName()).set(strategy_class);
            model.get(KeyspaceDefinition.REPLICATION_FACTOR.getName()).set(replication_factor);
            model.get(KeyspaceDefinition.NAME.getName()).set(name);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public PathElement getPathElement() {
            return path;
        }

        @Override
        public ModelNode getModel() {
            return model;
        }

        @Override
        public void writeModel(ModelNode newModel) {
            this.model = newModel;
        }

        @Override
        public boolean isModelDefined() {
            return this.model.isDefined();
        }

        @Override
        public boolean hasChild(PathElement element) {
            return false;
        }

        @Override
        public Resource getChild(PathElement element) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Resource requireChild(PathElement element) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean hasChildren(String childType) {
            return false;
        }

        @Override
        public Resource navigate(PathAddress address) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Set<String> getChildTypes() {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getChildrenNames(String childType) {
            return Collections.emptySet();
        }

        @Override
        public Set<ResourceEntry> getChildren(String childType) {
            return Collections.emptySet();
        }

        @Override
        public void registerChild(PathElement address, Resource resource) {
        }

        @Override
        public Resource removeChild(PathElement address) {
            return null;
        }

        @Override
        public boolean isRuntime() {
            return true;
        }

        @Override
        public boolean isProxy() {
            return false;
        }

        @Override
        public Resource clone() {
            return new KeyspaceResourceEntry(name, model.get(KeyspaceDefinition.CLASS.getName()).asString(),
                    model.get(KeyspaceDefinition.REPLICATION_FACTOR.getName()).asInt());
        }

        @Override
        public void registerChild(PathElement address, int index, Resource resource) {
        }

        @Override
        public Set<String> getOrderedChildTypes() {
            return Collections.emptySet();
        }

    }
}
