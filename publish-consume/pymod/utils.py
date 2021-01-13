import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter, BinaryEncoder, BinaryDecoder, DatumReader
from io import BytesIO

import sys


def load_schema(schema):
    try:
        f = open(schema)
        schema = avro.schema.parse(f.read())
        return schema
    except Exception as e:
        raise e


def body2dict(body):
    body_fields = ['summary', 'message', 'actual_data']
    return extract_body(body, body_fields)


def extract_body(body, fields, maps=None):
    msg = dict()

    bodylines = body.split('\n')
    for line in bodylines:
        split = line.split(': ', 1)
        if len(split) > 1:
            key = split[0]
            value = split[1]

            if key not in set(fields):
                continue

            if maps and key in maps:
                key = maps[key]

            msg[key] = value

    return msg


def tag2dict(body):
    tag_fields = ['vofqan', 'voname', 'roc', 'site']

    body_to_tagname = dict(site='endpoint_group')

    return extract_body(body, tag_fields, body_to_tagname)


def avro_serialize(msg, schemapath):
    schema = open(schemapath)
    avro_writer = DatumWriter(avro.schema.parse(schema.read()))
    bytesio = BytesIO()
    encoder = BinaryEncoder(bytesio)
    avro_writer.write(msg, encoder)
    return bytesio.getvalue()


def avro_deserialize(msg, schema, bulk=False):
    opened_schema = load_schema(schema)
    avro_reader = DatumReader(opened_schema)
    bytesio = BytesIO(msg)
    decoder = BinaryDecoder(bytesio)
    if bulk:
        data = []
        try:
            while True:
                data.append(avro_reader.read(decoder))
        except AssertionError:
            return data
    else:
        return avro_reader.read(decoder)
