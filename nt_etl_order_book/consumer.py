import asyncio

from postgres_client import PostgresClient
from redis_client import RedisClient
import polars as pl


class Consumer:
    def __init__(self, batch_size: int = 100):
        self.redis_client = RedisClient()
        self.postgres_client = PostgresClient()
        self.batch_size = batch_size

    async def run(self):
        """
        Run the consumer: connect to Postgres, initialize schema,
        process existing snapshots, then listen for new ones.
        """
        # Initialize database schema
        self.postgres_client.initialize_schema()
        print("Database schema initialized")

        # Process existing snapshots and deltas first
        await self._process_existing_snapshots()
        await self._process_existing_deltas()
        
        # Then listen for new snapshots
        # TODO

    async def _process_existing_snapshots(self):
        num_proccessed = 0
        start_id = "-"
        records = []
        processed_ids = []

        print("Processing snapshots")
        while True:
            messages = await self.redis_client.get_orderbook_snapshots(
                count=100, start_id=start_id
            )

            # Break if no more messages
            if not messages:
                break

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
                num_proccessed += 1
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

            print(f"Inserting {len(processed_ids)} snapshots into Postgres")
            self.postgres_client.insert_orderbook_snapshots(records_df)

            print(f"Deleting {len(processed_ids)} snapshots from Redis")
            if processed_ids:
                await self.redis_client.delete_messages(stream_key="orderbook:snapshot", message_ids=processed_ids)

    async def _process_existing_deltas(self):
        num_proccessed = 0
        start_id = "-"
        records = []
        processed_ids = []

        print("Processing deltas")
        while True:
            messages = await self.redis_client.get_orderbook_deltas(
                count=100, start_id=start_id
            )

            # Break if no more messages
            if not messages:
                break

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
                num_proccessed += 1
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

            print(f"Inserting {len(processed_ids)} deltas into Postgres")
            self.postgres_client.insert_orderbook_deltas(records_df)

            print(f"Deleting {len(processed_ids)} deltas from Redis")
            if processed_ids:
                await self.redis_client.delete_messages("orderbook:delta", processed_ids)


if __name__ == "__main__":
    asyncio.run(Consumer().run())
