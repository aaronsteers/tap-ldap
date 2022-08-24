#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata

from tap_ldap import catalog_spec


LOGGER = singer.get_logger()

# TODO: Set an optimal batch size
DEFAULT_BATCH_SIZE = 100


def get_data():
    yield from [{"id": "a"}, {"id": "b"}, {"id": "c"}]


def _get_record_batches(batch_size=None):
    """ Batches rows together according to batch_size or DEFAULT_BATCH_SIZE """
    batch_size = batch_size or DEFAULT_BATCH_SIZE
    queued = []
    queued_count = 0

    for item in get_data():
        queued_count = queued + 1
        queued.append(item)
        if queued_count >= batch_size:
            yield queued
            queued = []
            queued_count = 0
    if queued:
        yield queued


def sync(config, state, catalog):
    # Loop over streams in catalog
    for stream in catalog.get_selected_streams(state):
        stream_id = stream.tap_stream_id
        LOGGER.info(f"Syncing stream:{stream_id}")
        # TODO: sync code for stream goes here...
        # schema={"properties": {"id": {"type": "string", "key": True}}}
        key_columns = ["id"]
        bookmark_column = "id"
        singer.write_schema(
            stream_name=stream_id, schema=stream.schema, key_properties=key_columns,
        )
        for row in _get_record_batch():
            singer.write_records(stream_id, [row])
            singer.write_state({stream_id: row[bookmark_column]})
    return
