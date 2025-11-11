import asyncio

import polars as pl
from postgres_client import PostgresClient
from redis_client import RedisClient


class Consumer:
    def __init__(self, batch_size: int = 100):
        self.redis_client = RedisClient()
        self.postgres_client = PostgresClient()
        self.batch_size = batch_size

    async def run(self):
        """
        Run the consumer: connect to Postgres, initialize schema,
        and continuously process snapshots and deltas.
        """
        # Initialize database schema
        self.postgres_client.initialize_schema()
        print("Database schema initialized")

        # Process snapshots and deltas concurrently
        await asyncio.gather(
            self._process_snapshots(),
            self._process_deltas(),
        )

    async def _process_snapshots(self):
        """
        Continuously process orderbook snapshots from Redis stream.
        Handles both existing messages (backlog) and new incoming messages.
        """
        start_id = "-"
        num_processed = 0

        print("Starting snapshot processor")
        while True:
            records = []
            processed_ids = []

            messages = await self.redis_client.get_orderbook_snapshots(
                count=self.batch_size, start_id=start_id
            )

            # If no messages, wait and check again for new ones
            if not messages:
                await asyncio.sleep(0.1)
                continue

            for redis_stream_id, snapshot in messages:
                timestamp = snapshot["ingestion_ts"]
                ticker = snapshot["market_ticker"]
                yes_dollars = snapshot["yes_dollars"]
                no_dollars = snapshot["no_dollars"]

                # Process yes side
                for price_dollars, contracts in yes_dollars:
                    records.append(
                        {
                            "timestamp": timestamp,
                            "ticker": ticker,
                            "side": "yes",
                            "price_dollars": price_dollars,
                            "contracts": contracts,
                            "redis_stream_id": redis_stream_id,
                        }
                    )

                # Process no side
                for price_dollars, contracts in no_dollars:
                    records.append(
                        {
                            "timestamp": timestamp,
                            "ticker": ticker,
                            "side": "no",
                            "price_dollars": price_dollars,
                            "contracts": contracts,
                            "redis_stream_id": redis_stream_id,
                        }
                    )

                processed_ids.append(redis_stream_id)
                start_id = "(" + redis_stream_id

            if len(records) > 0:
                records_df = pl.DataFrame(records).cast(
                    {
                        "timestamp": pl.Int64,
                        "ticker": pl.String,
                        "side": pl.String,
                        "price_dollars": pl.Decimal(5, 4),
                        "contracts": pl.Int32,
                        "redis_stream_id": pl.String,
                    }
                )

                self.postgres_client.insert_orderbook_snapshots(records_df)
                num_processed += len(processed_ids)
                # print(
                #     f"Processed {len(processed_ids)} snapshots (total: {num_processed})"
                # )

                if processed_ids:
                    await self.redis_client.delete_messages(
                        stream_key="orderbook:snapshot", message_ids=processed_ids
                    )

    async def _process_deltas(self):
        """
        Continuously process orderbook deltas from Redis stream.
        Handles both existing messages (backlog) and new incoming messages.
        """
        start_id = "-"
        num_processed = 0

        print("Starting delta processor")
        while True:
            records = []
            processed_ids = []

            messages = await self.redis_client.get_orderbook_deltas(
                count=self.batch_size, start_id=start_id
            )

            # If no messages, wait and check again for new ones
            if not messages:
                await asyncio.sleep(0.1)
                continue

            for redis_stream_id, delta in messages:
                timestamp = delta["ingestion_ts"]
                ticker = delta["market_ticker"]
                side = delta["side"]
                price_dollars = delta["price_dollars"]
                delta_value = delta["delta"]

                records.append(
                    {
                        "timestamp": timestamp,
                        "ticker": ticker,
                        "side": side,
                        "price_dollars": price_dollars,
                        "delta": delta_value,
                        "redis_stream_id": redis_stream_id,
                    }
                )

                processed_ids.append(redis_stream_id)
                start_id = "(" + redis_stream_id

            if len(records) > 0:
                records_df = pl.DataFrame(records).cast(
                    {
                        "timestamp": pl.Int64,
                        "ticker": pl.String,
                        "side": pl.String,
                        "price_dollars": pl.Decimal(5, 4),
                        "delta": pl.Int32,
                        "redis_stream_id": pl.String,
                    }
                )

                self.postgres_client.insert_orderbook_deltas(records_df)
                num_processed += len(processed_ids)
                # print(f"Processed {len(processed_ids)} deltas (total: {num_processed})")

                if processed_ids:
                    await self.redis_client.delete_messages(
                        "orderbook:delta", processed_ids
                    )
