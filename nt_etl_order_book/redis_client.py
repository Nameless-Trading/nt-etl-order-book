import json
import os
import time

import redis.asyncio
from dotenv import load_dotenv


class RedisClient:
    def __init__(self, redis_public_url: str | None = None):
        load_dotenv(override=True)

        if redis_public_url is None:
            self._redis_public_url = os.getenv("REDIS_PUBLIC_URL")

        self._client = redis.asyncio.from_url(self._redis_public_url)

    async def save_orderbook_snapshot(self, message: dict) -> str:
        """
        Save an orderbook snapshot to a Redis stream.

        Stream key format: orderbook:snapshot:{market_ticker}

        Returns the message ID from Redis.
        """
        msg: dict = message.get("msg", {})
        market_ticker = str(msg.get("market_ticker"))

        if not market_ticker:
            raise ValueError("market_ticker not found in message")

        stream_key = f"orderbook:snapshot:{market_ticker}"

        # Prepare data for Redis stream
        # We'll store the entire message as JSON in a single field
        data = {
            "type": str(message.get("type")),
            "sid": str(message.get("sid")),
            "seq": str(message.get("seq")),
            "market_ticker": market_ticker,
            "market_id": str(msg.get("market_id")),
            "yes_dollars": json.dumps(msg.get("yes_dollars", [])),
            "no_dollars": json.dumps(msg.get("no_dollars", [])),
            "yes": json.dumps(msg.get("yes", [])),
            "no": json.dumps(msg.get("no", [])),
            "ingestion_ts": str(int(time.time() * 1000)),
        }

        # Add to Redis stream
        message_id = await self._client.xadd(stream_key, data)
        return (
            message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
        )

    async def save_orderbook_delta(self, message: dict) -> str:
        """
        Save an orderbook delta to a Redis stream.

        Stream key format: orderbook:delta:{market_ticker}

        Returns the message ID from Redis.
        """
        msg: dict = message.get("msg", {})
        market_ticker = str(msg.get("market_ticker"))

        if not market_ticker:
            raise ValueError("market_ticker not found in message")

        stream_key = f"orderbook:delta:{market_ticker}"

        # Prepare data for Redis stream
        data = {
            "type": str(message.get("type")),
            "sid": str(message.get("sid")),
            "seq": str(message.get("seq")),
            "market_ticker": market_ticker,
            "market_id": str(msg.get("market_id")),
            "price": str(msg.get("price")),
            "price_dollars": msg.get("price_dollars"),
            "delta": str(msg.get("delta")),
            "side": str(msg.get("side")),
            "ts": str(msg.get("ts")),
            "ingestion_ts": str(int(time.time() * 1000)),
        }

        # Add to Redis stream
        message_id = await self._client.xadd(stream_key, data)
        return (
            message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
        )

    async def close(self):
        """Close the Redis connection."""
        await self._client.close()
