#!/usr/bin/env python3

from argparse import ArgumentParser
from argo_ams_library import ArgoMessagingService, AmsException

import avro.schema
import os

from avro.datafile import DataFileWriter
from avro.io import DatumWriter, BinaryEncoder, BinaryDecoder, DatumReader
from io import BytesIO


def load_schema(schema):
    try:
        f = open(schema)
        schema = avro.schema.parse(f.read())
        return schema
    except Exception as e:
        raise e


def avro_deserialize(msg, schema):
    opened_schema = load_schema(schema)
    avro_reader = DatumReader(opened_schema)
    bytesio = BytesIO(msg.encode('utf-8'))
    decoder = BinaryDecoder(bytesio)
    return avro_reader.read(decoder)


def main():
    parser = ArgumentParser(description="Simple AMS example of subscription pull/consume")
    parser.add_argument('--host', type=str, default='messaging-devel.argo.grnet.gr', help='FQDN of AMS Service')
    parser.add_argument('--token', type=str, required=True, help='Given token')
    parser.add_argument('--project', type=str, required=True, help='Project  registered in AMS Service')
    parser.add_argument('--subscription', type=str, required=True, help='Subscription name')
    parser.add_argument('--topic', type=str, required=True, help='Given topic')
    parser.add_argument('--nummsgs', type=int, default=3, help='Number of messages to pull and ack')
    parser.add_argument('--schema', type=str, required=True, help='Avro schema')
    parser.add_argument('--outfile', type=str, required=True, help='Output avro file')
    args = parser.parse_args()

    # initialize service with given token and project
    ams = ArgoMessagingService(endpoint=args.host, token=args.token, project=args.project)

    # ensure that subscription is created in first run. messages can be
    # pulled from the subscription only when subscription already exists
    # for given topic prior messages being published to topic
    try:
        if not ams.has_sub(args.subscription):
            ams.create_sub(args.subscription, args.topic)
        subscription = ams.get_sub(args.subscription, retobj=True)
    except AmsException as e:
        print(e)
        raise SystemExit(1)

    # try to pull number of messages from subscription. method will
    # return (ackIds, AmsMessage) tuples from which ackIds and messages
    # payload will be extracted.
    avro_payloads = list()
    for msg in subscription.pullack(args.nummsgs, retry=5, retrysleep=15, timeout=5):
        data = msg.get_data()
        msgid = msg.get_msgid()
        print('msgid={0}'.format(msgid))
        avro_payloads.append(data)

    try:
        schema = load_schema(args.schema)
        if os.path.exists(args.outfile):
            avroFile = open(args.outfile, 'a+b')
            writer = DataFileWriter(avroFile, DatumWriter())
        else:
            avroFile = open(args.outfile, 'w+b')
            writer = DataFileWriter(avroFile, DatumWriter(), schema)

        for am in avro_payloads:
            msg = avro_deserialize(am, args.schema)
            writer.append(msg)

        writer.close()
        avroFile.close()

    except Exception as e:
        print(e)
        raise SystemExit(1)


main()
