#!/bin/bash

docker run \
--log-driver json-file \
--log-opt max-size=10m \
--name ams-broker-test \
-v $HOME:/mnt/ \
-v $HOME/.ssh:/home/user/.ssh/ \
-h docker-centos7 \
-v $HOME/my_work/srce/git.ams-test/ams-test/:/home/user/ams-test-source/ \
-v $HOME/my_work/srce/git.argo-ams-library/argo-ams-library/pymod/:/usr/lib/python2.7/site-packages/argo_ams_library/ \
-v $HOME/my_work/srce/git.argo-ams-library/argo-ams-library/pymod/:/usr/lib/python3.6/site-packages/argo_ams_library/ \
-v $HOME/my_work/srce/git.argo-ams-library/argo-ams-library:/home/user/argo-ams-library \
-v $HOME/my_work/srce/git.argo-ams-library/library_examples/:/home/user/argo-ams-library-examples \
--rm -ti -v /dev/log:/dev/log ipanema:5000/ams-broker-test
