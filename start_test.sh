#!/bin/sh
ant clean build jar
./bin/voldemort-server.sh config/test_node
