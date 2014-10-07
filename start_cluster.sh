#!/bin/sh

if [ $# != 1 ]; then
	echo "Need a server id [0-5]"
	exit
fi

echo $1

#rm config/cluster/node_$1/config/voldsys*
#rm -r config/cluster/node_$1/data/

./bin/voldemort-prod-server.sh config/cluster/node_$1
