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
import com.datastax.driver.core.Session;
import java.io.IOException;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;

/**
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class KeyspaceAddHandler extends AbstractAddStepHandler implements CassandraOperation {

    public static final KeyspaceAddHandler INSTANCE = new KeyspaceAddHandler();

    public KeyspaceAddHandler() {
        super(LISTEN_ADDRESS, NATIVE_TRANSPORT_PORT);
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        String keySpaceName = Util.getNameFromAddress(context.getCurrentAddress());
        ClusterResource cluster = (ClusterResource) context.readResourceFromRoot(context.getCurrentAddress().getParent());
        try (Session session = cluster.getSession()) {
            cluster.traceQuery("SELECT * FROM system_schema.keyspaces;");
            executeQuery(context, session,
                    String.format("CREATE KEYSPACE \"%1s\" WITH REPLICATION = {'class':'%2s', 'replication_factor': %3d}",
                            keySpaceName, CLASS.resolveModelAttribute(context, model).asString(),
                            REPLICATION_FACTOR.resolveModelAttribute(context, model).asInt(1)));
        } catch (IOException ex) {
            throw new OperationFailedException(ex);
        }
    }

    @Override
    protected boolean requiresRuntime(OperationContext context) {
        return !context.isBooting();
    }

    @Override
    public void processResult(OperationContext context, ResultSet rs) {
    }
}
