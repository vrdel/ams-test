#!/usr/bin/env python3

import argparse
import datetime
import random
import sys
import time

from argo_ams_library.ams import ArgoMessagingService
from argo_ams_library.amsmsg import AmsMessage
from argo_ams_library.amsexceptions import AmsException


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--topic', type=str, required=True, help='Given topic')
    args = parser.parse_args()

    ams = ArgoMessagingService(endpoint=args.host,
                               token=args.token,
                               project=args.project)
    msg = AmsMessage(attributes={'header': 'foo'}, data='foobarpy3')

    try:
        ams.publish(args.topic, msg)
    except AmsException as e:
        print(e)


main()
