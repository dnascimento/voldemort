#!/bin/bash

echo "<cluster>
  <name>darioCluster</name>
  <server>
    <id>0</id>
    <host>192.168.1.100</host>
    <http-port>8081</http-port>
    <socket-port>6666</socket-port>
    <admin-port>6667</admin-port>
    <partitions>0,1</partitions>
  </server>
</cluster>
" > config/cluster.xml
rm -r config/.*
rm config/voldsys*
rm -r data
