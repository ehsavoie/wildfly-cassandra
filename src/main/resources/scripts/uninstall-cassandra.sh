#!/bin/sh

set -o xtrace
WILDFLY_HOME=`dirname "$0"`

rm -f $WILDFLY_HOME/bin/nodetool
rm -rf $WILDFLY_HOME/modules/system/layers/base/org/wildfly/extension/cassandra/
rm -f $WILDFLY_HOME/standalone/configuration/standalone-cassandra.xml
rm -f $WILDFLY_HOME/domain/configuration/cassandra-domain.xml
rm -f $WILDFLY_HOME/domain/configuration/cassandra-host.xml
rm -f $WILDFLY_HOME/uninstall-cassandra.sh