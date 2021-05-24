#!/usr/bin/env python

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException

def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--topic', type=str, default=None, required=False, help='Given topic')
    args = parser.parse_args()

    # initialize service with given token and project
    ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)
    try:
        topic = ams.get_topic(args.topic, retobj=True)
        for s in topic.iter_subs():
            print s.name, s.fullname, s.topic.name
    except AmsException as e:
        print e
        raise SystemExit(1)
main()
