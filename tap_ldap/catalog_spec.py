import json
import os
import singer

from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_ldap import ldap_core


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(_get_abs_path("schemas")):
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover(detect=True):
    if detect:
        raw_schemas = [
            ldap_core.detect_schema(schema_name)
            for schema_name in ldap_core.SCHEMA_NAMES
        ]

    else:
        raw_schemas = catalog_spec.load_schemas()
    streams = []
    for schema_name, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []

        streams.append(
            CatalogEntry(
                tap_stream_id=schema_name,
                stream=schema_name,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)
