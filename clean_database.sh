#!/bin/sh

./bin/voldemort-admin-tool.sh --truncate test --url tcp://localhost:6666
./bin/voldemort-admin-tool.sh --truncate questionStore --url tcp://localhost:6666
./bin/voldemort-admin-tool.sh --truncate answerStore --url tcp://localhost:6666
./bin/voldemort-admin-tool.sh --truncate commentStore --url tcp://localhost:6666
./bin/voldemort-admin-tool.sh --truncate index --url tcp://localhost:6666
