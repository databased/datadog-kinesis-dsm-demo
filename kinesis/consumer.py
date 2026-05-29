"""Kinesis DSM consumer -- reads events from a Kinesis stream and emits DSM trace spans.

Instrumented with Datadog Data Streams Monitoring (DSM) via dd-trace botocore patch.

DSM requirement: DD_DATA_STREAMS_ENABLED=true must be set before boto3 import.
DSM requirement: StreamARN must be passed to every GetRecords/ListShards/GetShardIterator call.

CONSUMER_LAG_SECONDS env var: set to 2-5 to simulate consumer lag for demo purposes.
"""
import json
import logging
import os
import time

from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Set DD env vars before importing boto3 -- dd-trace patches botocore on import
os.environ.setdefault('DD_DATA_STREAMS_ENABLED', 'true')
os.environ.setdefault('DD_SERVICE', 'kinesis-dsm-consumer')
os.environ.setdefault('DD_ENV', os.environ.get('DD_ENV', 'demo'))

import ddtrace  # noqa: E402 -- must follow env var setup
ddtrace.patch_all()

import boto3  # noqa: E402 -- must follow ddtrace.patch_all()

log = logging.getLogger(__name__)

STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
# StreamARN required for DSM checkpoint propagation -- StreamName alone causes silent failures
STREAM_ARN = os.environ['KINESIS_STREAM_ARN']
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
LAG_SECONDS = float(os.environ.get('CONSUMER_LAG_SECONDS', '0'))
POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL_SECONDS', '0.5'))

kinesis = boto3.client('kinesis', region_name=AWS_REGION)


def get_shards() -> list[str]:
    """Returns the list of shard IDs for the configured Kinesis stream.

    Returns:
        A list of shard ID strings.

    Raises:
        ClientError: If the Kinesis ListShards call fails.
    """
    try:
        response = kinesis.list_shards(
            StreamName=STREAM_NAME,
            StreamARN=STREAM_ARN,           # Required for DSM
        )
    except ClientError as err:
        log.error("Kinesis ListShards failed: %s", err)
        raise
    return [s['ShardId'] for s in response['Shards']]


def get_shard_iterator(shard_id: str) -> str:
    """Returns a LATEST shard iterator for the given shard ID.

    Args:
        shard_id: The Kinesis shard identifier.

    Returns:
        A shard iterator string pointing to the latest position in the shard.

    Raises:
        ClientError: If the Kinesis GetShardIterator call fails.
    """
    try:
        response = kinesis.get_shard_iterator(
            StreamName=STREAM_NAME,
            StreamARN=STREAM_ARN,           # Required for DSM
            ShardId=shard_id,
            ShardIteratorType='LATEST',
        )
    except ClientError as err:
        log.error("Kinesis GetShardIterator failed for shard %s: %s", shard_id, err)
        raise
    return response['ShardIterator']


def consume() -> None:
    """Polls all shards continuously and logs each consumed event.

    Reads from LATEST position on startup. Optionally sleeps LAG_SECONDS per
    record to simulate a slow consumer for DSM lag demo purposes.
    """
    shard_ids = get_shards()
    iterators = {sid: get_shard_iterator(sid) for sid in shard_ids}
    log.info("Consumer starting -- stream: %s, shards: %d", STREAM_NAME, len(shard_ids))
    log.info("DSM enabled: %s", os.environ.get('DD_DATA_STREAMS_ENABLED'))
    if LAG_SECONDS > 0:
        log.info("LAG mode: sleeping %ss per record (simulating slow consumer)", LAG_SECONDS)

    count = 0
    while True:
        for shard_id in list(iterators.keys()):
            iterator = iterators[shard_id]
            if not iterator:
                continue
            try:
                response = kinesis.get_records(
                    ShardIterator=iterator,
                    StreamARN=STREAM_ARN,   # Required for DSM
                    Limit=10,
                )
            except ClientError as err:
                log.error("Kinesis GetRecords failed for shard %s: %s", shard_id, err)
                raise
            for record in response['Records']:
                event = json.loads(record['Data'].decode('utf-8'))
                count += 1
                log.info("[%d] Consumed %s user=%s",
                         count, event['event_type'], event['user_id'])
                if LAG_SECONDS > 0:
                    time.sleep(LAG_SECONDS)
            iterators[shard_id] = response.get('NextShardIterator')
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    consume()
