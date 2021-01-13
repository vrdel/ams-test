#!/bin/bash

docker run \
--log-driver json-file \
--log-opt max-size=10m \
--name ams-broker-test \
-v $HOME:/mnt/ \
-v $HOME/.ssh:/home/user/.ssh/ \
-h docker-centos7 \
-v $HOME/my_work/srce/git.ams-test/ams-test/:/home/user/bin/ \
--rm -ti -v /dev/log:/dev/log ipanema:5000/ams-broker-test
