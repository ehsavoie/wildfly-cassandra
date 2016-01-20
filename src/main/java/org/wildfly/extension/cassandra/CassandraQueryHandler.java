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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.io.IOException;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinition;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class CassandraQueryHandler implements OperationStepHandler, CassandraOperation {

    public static final CassandraQueryHandler INSTANCE = new CassandraQueryHandler();

    public static final SimpleAttributeDefinition QUERY = SimpleAttributeDefinitionBuilder
            .create("query", ModelType.STRING, false)
            .setAllowExpression(false)
            .build();

     public static final SimpleAttributeDefinition KEYSPACE = SimpleAttributeDefinitionBuilder
            .create("keyspace", ModelType.STRING, true)
            .setAllowExpression(false)
            .build();

    public static final StringListAttributeDefinition RESULT_SET = new StringListAttributeDefinition.Builder("result_set")
            .setAllowNull(true)
            .build();

    public static final SimpleOperationDefinition DEFINITION = new SimpleOperationDefinitionBuilder("execute",
            CassandraExtension.getResourceDescriptionResolver())
            .setReadOnly()
            .setRuntimeOnly()
            .setParameters(QUERY, KEYSPACE)
            .setReplyType(ModelType.LIST)
            .setReplyParameters(RESULT_SET).build();

    @Override
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        final String query = QUERY.resolveModelAttribute(context, operation).asString();
        final ModelNode keyspaceModel = KEYSPACE.resolveModelAttribute(context, operation);
        context.addStep(new OperationStepHandler() {
            @Override
            public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
                ClusterResource resource = (ClusterResource)context.readResource(PathAddress.EMPTY_ADDRESS);
                try (Session session = keyspaceModel.isDefined() ? resource.getSession(keyspaceModel.asString()) : resource.getSession()) {
                    resource.traceQuery(query);
                    try {
                        executeQuery(context, session, query);
                    } catch (IOException ex) {
                        throw new OperationFailedException(ex);
                    }
                }
            }
        }, OperationContext.Stage.RUNTIME);
    }

    @Override
    public void processResult(OperationContext context, ResultSet rs) {
        ModelNode resultSetModel = context.getResult().get(RESULT_SET.getName()).setEmptyList();
        for(Row row : rs.all()) {
            resultSetModel.add(row.toString());
        }
    }
    
}
