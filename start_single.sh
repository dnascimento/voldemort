#!/bin/sh

if [ $# != 0 ]; then
	ant clean build jar
fi

rm config/single_node/config/voldsys*
rm -r config/single_node/data/

./bin/voldemort-prod-server.sh config/single_node

