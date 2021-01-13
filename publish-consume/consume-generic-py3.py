#!/usr/bin/env python3

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException

import os

from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from pymod.utils import avro_deserialize, load_schema


def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--subscription', type=str, required=True, help='Subscription name')
    args = parser.parse_args()

    # initialize service with given token and project
    ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)

    for msg in ams.pullack_sub(args.subscription, retry=5, retrysleep=15, return_immediately=True):
        try:
            data = msg.get_data()
            msgid = msg.get_msgid()
            print('msgid={0}'.format(msgid))
            print('msgdata={0}'.format(data))
        except AmsException as e:
            print(e)
            raise SystemExit(1)
main()
