#!/usr/bin/env python

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException

def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--topic', type=str, required=True, help='Given topic')
    parser.add_argument('--subscription', type=str, required=True, help='Given subscription')
    args = parser.parse_args()

    topic = None
    # initialize service with given token and project
    try:
        ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)
        topic = ams.topic(args.topic)
        sub = topic.subscription(args.subscription)
        print sub.acl()
        sub.acl(["test-publisher01"])
        print sub.acl()
    except AmsException as e:
        print e
        raise SystemExit(1)



main()
