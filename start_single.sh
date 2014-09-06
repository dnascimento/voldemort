#!/bin/sh

if [ $# != 0 ]; then
	ant clean build jar
fi

./bin/voldemort-server.sh config/single_node
