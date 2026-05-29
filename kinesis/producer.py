"""Kinesis DSM producer -- publishes synthetic e-commerce events to a Kinesis stream.

Instrumented with Datadog Data Streams Monitoring (DSM) via dd-trace botocore patch.

DSM requirement: DD_DATA_STREAMS_ENABLED=true must be set before boto3 import.
DSM requirement: StreamARN must be passed to every PutRecord/PutRecords call.
"""
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Any

from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Set DD env vars before importing boto3 -- dd-trace patches botocore on import
os.environ.setdefault('DD_DATA_STREAMS_ENABLED', 'true')
os.environ.setdefault('DD_SERVICE', 'kinesis-dsm-producer')
os.environ.setdefault('DD_ENV', os.environ.get('DD_ENV', 'demo'))

import ddtrace  # noqa: E402 -- must follow env var setup
ddtrace.patch_all()

import boto3  # noqa: E402 -- must follow ddtrace.patch_all()

log = logging.getLogger(__name__)

STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
# StreamARN required for DSM checkpoint propagation -- StreamName alone causes silent failures
STREAM_ARN = os.environ['KINESIS_STREAM_ARN']
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
PUBLISH_INTERVAL = float(os.environ.get('PUBLISH_INTERVAL_SECONDS', '1.0'))

kinesis = boto3.client('kinesis', region_name=AWS_REGION)

EVENT_TYPES = [
    'order.placed',
    'order.shipped',
    'order.delivered',
    'user.signup',
    'payment.processed',
    'cart.abandoned',
]


def make_event() -> dict[str, Any]:
    """Creates a synthetic e-commerce event with random field values."""
    return {
        'event_type': random.choice(EVENT_TYPES),
        'user_id': random.randint(1000, 9999),
        'order_id': random.randint(100000, 999999),
        'amount': round(random.uniform(10.0, 500.0), 2),
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }


def publish_event(event: dict[str, Any]) -> None:
    """Publishes a single event to the Kinesis stream.

    Args:
        event: The event payload to serialize and publish.

    Raises:
        ClientError: If the Kinesis PutRecord call fails.
    """
    try:
        kinesis.put_record(
            StreamName=STREAM_NAME,
            StreamARN=STREAM_ARN,           # Required for DSM context propagation
            Data=json.dumps(event).encode('utf-8'),
            PartitionKey=str(event['user_id']),
        )
    except ClientError as err:
        log.error("Kinesis PutRecord failed: %s", err)
        raise


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    log.info("Producer starting -- stream: %s", STREAM_NAME)
    log.info("DSM enabled: %s", os.environ.get('DD_DATA_STREAMS_ENABLED'))
    count = 0
    while True:
        event = make_event()
        publish_event(event)
        count += 1
        log.info("[%d] Published %s user=%s amount=$%s",
                 count, event['event_type'], event['user_id'], event['amount'])
        time.sleep(PUBLISH_INTERVAL)
