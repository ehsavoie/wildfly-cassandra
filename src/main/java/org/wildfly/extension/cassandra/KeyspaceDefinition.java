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



import static org.wildfly.extension.cassandra.CassandraModel.KEYSPACE;

import java.util.Arrays;
import java.util.Collection;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.PersistentResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class KeyspaceDefinition extends PersistentResourceDefinition {

    static final AttributeDefinition CLASS = SimpleAttributeDefinitionBuilder.create("class", ModelType.STRING)
                    .setAllowNull(true)
                    .setAllowExpression(false)
                    .setMaxSize(20)
                    .setDefaultValue(new ModelNode("SimpleStrategy")).build();

    static final AttributeDefinition REPLICATION_FACTOR = SimpleAttributeDefinitionBuilder.create("replication_factor", ModelType.INT)
                    .setAllowNull(true)
                    .setAllowExpression(false)
                    .setStorageRuntime()
                    .setDefaultValue(new ModelNode(1)).build();

    static final AttributeDefinition NAME = SimpleAttributeDefinitionBuilder.create(ModelDescriptionConstants.NAME, ModelType.STRING)
                    .setAllowNull(false)
                    .setStorageRuntime()
                    .setAllowExpression(false)
                    .setMaxSize(20).build();

    static final AttributeDefinition[] ATTRIBUTES = {NAME, CLASS, REPLICATION_FACTOR};

    public static final KeyspaceDefinition INSTANCE= new KeyspaceDefinition();

    private KeyspaceDefinition() {
        super(PathElement.pathElement(KEYSPACE),
                CassandraExtension.getResourceDescriptionResolver(CassandraModel.CLUSTER, CassandraModel.KEYSPACE),
                KeyspaceAddHandler.INSTANCE, KeyspaceRemoveHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        for (AttributeDefinition attr : getAttributes()) {
            resourceRegistration.registerReadOnlyAttribute(attr, null);
        }
    }

    @Override
    public Collection<AttributeDefinition> getAttributes() {
         return Arrays.asList(ATTRIBUTES);
    }

    @Override
    public boolean isRuntime() {
        return true;
    }
}
