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

import com.datastax.driver.core.ResultSet;
import org.jboss.as.controller.ControlledProcessState;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;

public class KeyspaceRemoveHandler extends CassandraOperationStepHandler {
    public static final KeyspaceRemoveHandler INSTANCE = new KeyspaceRemoveHandler();

    @Override
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        String keySpaceName = Util.getNameFromAddress(context.getCurrentAddress());
        ClusterResource cluster = (ClusterResource)context.readResourceFromRoot(context.getCurrentAddress().getParent());
        ModelNode clusterNode = cluster.getModel();
        String connectionPoint = LISTEN_ADDRESS.resolveModelAttribute(context, clusterNode).asString();
        int port = NATIVE_TRANSPORT_PORT.resolveModelAttribute(context, clusterNode).asInt();
        if(cluster.getProcessState().getCurrentState() == ControlledProcessState.State.RELOAD_REQUIRED) {
            connectionPoint = cluster.getConnectionPoint();
            port = cluster.getPort();
        }
        executeQuery(context, connectionPoint, port, String.format("DROP KEYSPACE \"%1s\"", keySpaceName));
        context.removeResource(PathAddress.EMPTY_ADDRESS);
    }

    @Override
    public void processResult(OperationContext context, ResultSet rs) {
    }

}
