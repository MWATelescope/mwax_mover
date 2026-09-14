"""Pushing messages onto a Redis list.

push_message_to_redis() JSON-serialises a message and LPUSHes it onto a
Redis list, with a small retry loop on RedisError.
"""

import json
import logging
import time

import redis

logger = logging.getLogger(__name__)


def push_message_to_redis(redis_host: str, redis_queue_key: str, message_data):
    """
    Push a message onto a Redis list as a JSON-serialised string.

    Connects to Redis on port 6379 of ``redis_host``, serialises
    ``message_data`` to JSON, and pushes it onto the left end of the list at
    ``redis_queue_key`` using ``LPUSH``. Retries up to ``MAX_RETRIES`` (3)
    times with a 1-second delay on ``RedisError``.

    Args:
        redis_host: Hostname or IP address of the Redis server.
        redis_queue_key: The Redis list key to push the message onto.
        message_data: Any JSON-serialisable Python object to push as the message.

    Raises:
        redis.RedisError: If the push fails on all retry attempts.
    """
    MAX_RETRIES = 3

    json_message = json.dumps(message_data)

    # Connect to Redis
    attempt = 1
    while attempt <= MAX_RETRIES:
        try:
            with redis.Redis(host=redis_host, port=6379, decode_responses=True) as r:
                r.lpush(redis_queue_key, json_message)
                return

        except redis.RedisError as e:
            # wait 1 second between retries
            time.sleep(1)

            if attempt >= MAX_RETRIES:
                raise redis.RedisError(
                    f"Could not push message to Redis host {redis_host} [{redis_queue_key}] after {attempt} tries: {e}"
                ) from e

        attempt += 1
