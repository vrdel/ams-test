#!/usr/bin/env python

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException

def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--subscription', type=str, required=True, help='Subscription name')
    parser.add_argument('--topic', type=str, required=True, help='Topic name')
    args = parser.parse_args()

    # initialize service with given token and project
    try:
        ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)
        if not ams.has_topic(args.topic):
            ams.create_topic(args.topic)
        topic = ams.get_topic(args.topic, retobj=True)
        sub = topic.subscription(args.subscription)
        msg = sub.pullack(3, timeout=5)
        if msg:
            print(msg[0].get_msgid())
    except AmsException as e:
        print e
        raise SystemExit(1)

main()
