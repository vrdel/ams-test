#!/usr/bin/env python

import logging

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException

log = logging.getLogger('argo_ams_library')
log.setLevel(logging.DEBUG)
log.addHandler(logging.handlers.SysLogHandler('/dev/log', logging.handlers.SysLogHandler.LOG_USER))
log.addHandler(logging.StreamHandler())

def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    args = parser.parse_args()

    # initialize service with given token and project
    try:
        ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)
        for t in ams.iter_topics(timeout=5):
            print(t.name, t.fullname)
    except AmsException as e:
        print('ERROR!')
        print(e)
        raise SystemExit(1)

    print('Second\n')

    for t in ams.iter_topics():
        print (t.name, t.fullname)

    print('Third\n')

    for t in ams.iter_topics():
        print (t.name, t.fullname)

main()
