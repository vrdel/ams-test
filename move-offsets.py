#!/usr/bin/env python3

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException


def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--subscription', type=str, required=True, help='Subscription name')
    parser.add_argument('--advance', required=False, type=int, default=0, help='Number of messages to pull and ack')
    args = parser.parse_args()

    # initialize service with given token and project
    ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)

    if args.advance:
        try:
            print(ams.getoffsets_sub(args.subscription))
            print(ams.modifyoffset_sub(args.subscription, args.advance))
        except AmsException as e:
            print(e)
            raise SystemExit(1)


main()
