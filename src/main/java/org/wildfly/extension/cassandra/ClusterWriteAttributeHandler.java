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

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.RestartParentWriteAttributeHandler;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceName;


/**
 * @author Heiko Braun
 * @since 20/08/14
 */
public class ClusterWriteAttributeHandler extends RestartParentWriteAttributeHandler {

    ClusterWriteAttributeHandler(AttributeDefinition... attributeDefinitions) {
        super(CassandraModel.CLUSTER, attributeDefinitions);
    }

    @Override
    protected ServiceName getParentServiceName(PathAddress parentAddress) {
        return CassandraService.BASE_SERVICE_NAME.append(Util.getNameFromAddress(parentAddress));
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress parentAddress, ModelNode parentModel) throws OperationFailedException {
        ClusterAdd.installRuntimeServices(context, parentAddress, parentModel);
    }
}
