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

import static org.wildfly.extension.cassandra.ClusterDefinition.LISTEN_ADDRESS;
import static org.wildfly.extension.cassandra.ClusterDefinition.NATIVE_TRANSPORT_PORT;
import static org.wildfly.extension.cassandra.KeyspaceDefinition.CLASS;
import static org.wildfly.extension.cassandra.KeyspaceDefinition.REPLICATION_FACTOR;

import com.datastax.driver.core.ResultSet;
import org.jboss.as.controller.ControlledProcessState;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;

/**
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class KeyspaceAddHandler extends CassandraOperationStepHandler {

    public static final KeyspaceAddHandler INSTANCE = new KeyspaceAddHandler();

    @Override
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        ModelNode model = new ModelNode();
        String keySpaceName = Util.getNameFromAddress(context.getCurrentAddress());
        KeyspaceDefinition.REPLICATION_FACTOR.validateAndSet(operation, model);
        KeyspaceDefinition.CLASS.validateAndSet(operation, model);
        Resource resource = context.createResource(PathAddress.EMPTY_ADDRESS);
        resource.writeModel(model);
        ClusterResource cluster = (ClusterResource)context.readResourceFromRoot(context.getCurrentAddress().getParent());
        ModelNode clusterNode = cluster.getModel();
        String connectionPoint = LISTEN_ADDRESS.resolveModelAttribute(context, clusterNode).asString();
        int port = NATIVE_TRANSPORT_PORT.resolveModelAttribute(context, clusterNode).asInt();
        if(cluster.getProcessState().getCurrentState() == ControlledProcessState.State.RELOAD_REQUIRED) {
            connectionPoint = cluster.getConnectionPoint();
            port = cluster.getPort();
        }
        executeQuery(context, connectionPoint, port,
                String.format("CREATE KEYSPACE \"%1s\" WITH REPLICATION = {'class':'%2s', 'replication_factor': %3d}",
                    keySpaceName, CLASS.resolveModelAttribute(context, model).asString(),
                    REPLICATION_FACTOR.resolveModelAttribute(context, model).asInt(1)));
    }

    @Override
    public void processResult(OperationContext context, ResultSet rs) {
    }
}
