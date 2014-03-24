#!/bin/sh
ant build
./bin/voldemort-server.sh config/single_node
