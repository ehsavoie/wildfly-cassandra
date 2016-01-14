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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 *
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2015 Red Hat, inc.
 */
public class CassandraConnectionPoint implements Closeable {

    private Cluster cluster;
    private final String key;

    public CassandraConnectionPoint(String node, int port, boolean debug) {
        key = node + ':' + port;
        connect(node, port, debug);
    }

    private final void connect(String node, int port, boolean debug) {
        cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        if (debug) {
            Metadata metadata = getMetadata();
            CassandraLogger.LOGGER.infof("Connected to cluster: %s\n", metadata.getClusterName());
            for (Host host : metadata.getAllHosts()) {
                CassandraLogger.LOGGER.infof("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
            }
        }
    }

    public Session getSession() {
        assert cluster != null;
        return cluster.connect();
    }

    public Metadata getMetadata() {
        assert cluster != null;
        return cluster.getMetadata();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.key);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CassandraConnectionPoint other = (CassandraConnectionPoint) obj;
        if (!Objects.equals(this.key, other.key)) {
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if(cluster != null) {
            cluster.close();
        }
    }
}
