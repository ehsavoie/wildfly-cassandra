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


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import java.io.IOException;
import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;

public class KeyspaceRemoveHandler extends AbstractRemoveStepHandler implements CassandraOperation {
    public static final KeyspaceRemoveHandler INSTANCE = new KeyspaceRemoveHandler();

    @Override
    protected void performRemove(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        super.performRemove(context, operation, model);
        String keySpaceName = Util.getNameFromAddress(context.getCurrentAddress());
        ClusterResource cluster = (ClusterResource)context.readResourceFromRoot(context.getCurrentAddress().getParent());
        try (Session session = cluster.getSession()) {
            executeQuery(context, session, String.format("DROP KEYSPACE %1s", keySpaceName));
        } catch (IOException ex) {
           throw new OperationFailedException(ex);
        }
    }


    @Override
    public void processResult(OperationContext context, ResultSet rs) {
    }

}
