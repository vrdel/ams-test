#!/usr/bin/env python

import argparse
import datetime
import messaging.generator as generator
import random
import sys
import time

from messaging.message import Message
from messaging.error import MessageError

from argo_ams_library.ams import ArgoMessagingService
from argo_ams_library.amsmsg import AmsMessage
from argo_ams_library.amsexceptions import AmsConnectionException, AmsServiceException

from pytz import timezone, UnknownTimeZoneError

from pymod.utils import avro_serialize, body2dict, tag2dict


def construct_msg(session, bodysize, timezone, schemapath):
    statusl = ['OK', 'WARNING', 'MISSING', 'CRITICAL', 'UNKNOWN', 'DOWNTIME']

    try:
        msg = Message()
        msg.header = dict()
        msg.body = str()

        if session:
            msg.header.update({'*** SESSION ***': '*** {0} ***'.format(session)})
        msg.header.update({'service': generator.rndb64(10)})
        msg.header.update({'hostname': generator.rndb64(10)})
        msg.header.update({'metric': generator.rndb64(10)})
        msg.header.update({'monitoring_host': generator.rndb64(10)})
        msg.header.update({'timestamp': str(datetime.datetime.now(timezone).strftime('%Y-%m-%dT%H:%M:%SZ'))})
        msg.header.update({'status': random.choice(statusl)})

        msg.body += 'summary: %s\n' % generator.rndb64(20)
        msg.body += 'message: %s\n' % generator.rndb64(bodysize)
        msg.body += 'vofqan: %s\n' % generator.rndb64(10)
        msg.body += 'actual_data: %s\n' % generator.rndb64(10)
        msg.body += 'voname: %s\n' % generator.rndb64(3)
        msg.body += 'roc: %s\n' % generator.rndb64(3)

        plainmsg = dict()
        plainmsg.update(msg.header)
        plainmsg.update(body2dict(msg.body))
        plainmsg.update(tags=tag2dict(msg.body))

        return avro_serialize(plainmsg, schemapath)

    except MessageError as e:
        sys.stderr.write('Error constructing message - %s\n', repr(e))

    else:
        return msg


def send(msg, host, project, topic, token):
    def _part_date():
        import datetime

        part_date_fmt = '%Y-%m-%d'
        d = datetime.datetime.now()

        return d.strftime(part_date_fmt)

    ams = ArgoMessagingService(endpoint=host,
                               token=token,
                               project=project)
    msg = AmsMessage(attributes={'partition_date': _part_date(),
                                 'type': 'metric_data'},
                     data=msg)
    try:
        ams.publish(topic, msg)
    except (AmsServiceException, AmsConnectionException) as e:
        print(e)
        raise SystemExit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--subscription', type=str, required=True, help='Subscription name')
    parser.add_argument('--topic', type=str, required=True, help='Given topic')
    parser.add_argument('--session', required=False, default=str(), type=str)
    parser.add_argument('--num', required=False, default=0, type=int)
    parser.add_argument('--sleep', required=False, default=0, type=float)
    parser.add_argument('--bodysize', required=False, default=40, type=int)
    parser.add_argument('--timezone', required=False, default='UTC', type=str)
    parser.add_argument('--schema', type=str, required=True, help='Avro schema')
    parser.add_argument('--noout', required=False, action='store_true', default=False)
    args = parser.parse_args()

    try:
        tz = timezone(args.timezone)
    except UnknownTimeZoneError as e:
        print("Timezone not correct")
        raise SystemExit(1)

    try:
        if args.num:
            for i in range(args.num):
                msg = construct_msg(args.session, args.bodysize, tz, args.schema)
                send(msg, args.host, args.project, args.topic, args.token)
                if not args.noout:
                    print(msg)
        else:
            while True:
                msg = construct_msg(args.session, args.bodysize, tz, args.schema)
                send(msg, args.host, args.project, args.topic, args.token)
                if not args.noout:
                    print(msg)
                if args.sleep:
                    time.sleep(args.sleep)

    except KeyboardInterrupt as e:
        raise SystemExit(0)


main()
