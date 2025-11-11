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

        Stream key format: orderbook:snapshot (consolidated stream)

        Returns the message ID from Redis.
        """
        msg: dict = message.get("msg", {})
        market_ticker = msg.get("market_ticker")

        if not market_ticker:
            raise ValueError("market_ticker not found in message")

        stream_key = "orderbook:snapshot"

        # Prepare data for Redis stream
        # Store numeric values as-is, only stringify what's necessary
        data = {
            "type": message.get("type"),
            "sid": message.get("sid"),
            "seq": message.get("seq"),
            "market_ticker": market_ticker,
            "market_id": msg.get("market_id"),
            "yes_dollars": json.dumps(msg.get("yes_dollars", [])),
            "no_dollars": json.dumps(msg.get("no_dollars", [])),
            "yes": json.dumps(msg.get("yes", [])),
            "no": json.dumps(msg.get("no", [])),
            "ingestion_ts": int(time.time() * 1000),
        }

        # Add to Redis stream
        message_id = await self._client.xadd(stream_key, data)
        return (
            message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
        )

    async def save_orderbook_delta(self, message: dict) -> str:
        """
        Save an orderbook delta to a Redis stream.

        Stream key format: orderbook:delta (consolidated stream)

        Returns the message ID from Redis.
        """
        msg: dict = message.get("msg", {})
        market_ticker = msg.get("market_ticker")

        if not market_ticker:
            raise ValueError("market_ticker not found in message")

        stream_key = "orderbook:delta"

        # Prepare data for Redis stream
        # Store numeric values as-is, only stringify what's necessary
        data = {
            "type": message.get("type"),
            "sid": message.get("sid"),
            "seq": message.get("seq"),
            "market_ticker": market_ticker,
            "market_id": msg.get("market_id"),
            "price": msg.get("price"),
            "price_dollars": msg.get("price_dollars"),
            "delta": msg.get("delta"),
            "side": msg.get("side"),
            "ts": msg.get("ts"),
            "ingestion_ts": int(time.time() * 1000),
        }

        # Add to Redis stream
        message_id = await self._client.xadd(stream_key, data)
        return (
            message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
        )

    async def get_orderbook_snapshots(
        self, count: int = 10, start_id: str = "-", end_id: str = "+"
    ) -> list[tuple[str, dict]]:
        """
        Get orderbook snapshots from the Redis stream.

        Args:
            count: Maximum number of snapshots to retrieve (default: 10)
            start_id: Starting message ID (default: "-" for beginning of stream)
            end_id: Ending message ID (default: "+" for end of stream)

        Returns:
            List of tuples containing (message_id, data_dict) where data_dict
            contains the snapshot fields with JSON fields parsed back into objects.
        """
        stream_key = "orderbook:snapshot"

        # Read from the stream
        messages = await self._client.xrange(stream_key, start_id, end_id, count)

        # Parse the results
        results = []
        for message_id, data in messages:
            # Decode bytes to strings
            message_id = (
                message_id.decode("utf-8")
                if isinstance(message_id, bytes)
                else message_id
            )

            # Decode and parse the data
            parsed_data = {}
            for key, value in data.items():
                key = key.decode("utf-8") if isinstance(key, bytes) else key
                value = value.decode("utf-8") if isinstance(value, bytes) else value

                # Parse JSON fields back to lists
                if key in ["yes_dollars", "no_dollars", "yes", "no"]:
                    parsed_data[key] = json.loads(value)
                else:
                    parsed_data[key] = value

            results.append((message_id, parsed_data))

        return results

    async def get_orderbook_deltas(
        self, count: int = 10, start_id: str = "-", end_id: str = "+"
    ) -> list[tuple[str, dict]]:
        """
        Get orderbook deltas from the Redis stream.

        Args:
            count: Maximum number of deltas to retrieve (default: 10)
            start_id: Starting message ID (default: "-" for beginning of stream)
            end_id: Ending message ID (default: "+" for end of stream)

        Returns:
            List of tuples containing (message_id, data_dict) where data_dict
            contains the delta fields (price, delta, side, etc.).
        """
        stream_key = "orderbook:delta"

        # Read from the stream
        messages = await self._client.xrange(stream_key, start_id, end_id, count)

        # Parse the results
        results = []
        for message_id, data in messages:
            # Decode bytes to strings
            message_id = (
                message_id.decode("utf-8")
                if isinstance(message_id, bytes)
                else message_id
            )

            # Decode the data
            parsed_data = {}
            for key, value in data.items():
                key = key.decode("utf-8") if isinstance(key, bytes) else key
                value = value.decode("utf-8") if isinstance(value, bytes) else value
                parsed_data[key] = value

            results.append((message_id, parsed_data))

        return results

    async def delete_message(self, stream_key: str, message_id: str) -> int:
        print(stream_key, message_id)
        return await self._client.xdel(stream_key, message_id)

    async def delete_messages(self, stream_key: str, message_ids: list[str]) -> int:
        """
        Delete multiple messages from a Redis stream in a single command.

        Args:
            stream_key: The Redis stream key
            message_ids: List of message IDs to delete

        Returns:
            Number of messages successfully deleted
        """
        if not message_ids:
            return 0
        return await self._client.xdel(stream_key, *message_ids)

    async def close(self):
        """Close the Redis connection."""
        await self._client.close()


if __name__ == "__main__":
    import asyncio
    from rich import print
    from postgres_client import PostgresClient

    async def main():
        redis_client = RedisClient()
        postgres_client = PostgresClient()

        postgres_client.initialize_schema()

        snapshots = await redis_client.get_orderbook_snapshots(count=1)

        for id, snapshot in snapshots:
            postgres_client.insert_orderbook_snapshots(
                redis_stream_id=id,
                timestamp=snapshot["ingestion_ts"],
                ticker=snapshot["market_ticker"],
                yes_dollars=snapshot["yes_dollars"],
                no_dollars=snapshot["no_dollars"],
            )

    asyncio.run(main())
