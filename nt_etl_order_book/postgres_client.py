import os

import psycopg2
from dotenv import load_dotenv
import polars as pl


class PostgresClient:
    def __init__(self, database_url: str | None = None):
        load_dotenv(override=True)

        if database_url is None:
            self._database_url = os.getenv("DATABASE_PUBLIC_URL")
        else:
            self._database_url = database_url

        # Connect immediately on creation
        self._conn = psycopg2.connect(self._database_url)

    def __del__(self):
        """Close connection when object is deleted."""
        self._conn.close()

    def initialize_schema(self):
        """Initialize database tables."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    timestamp BIGINT NOT NULL,
                    ticker VARCHAR(50) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    price_dollars DECIMAL(5, 4) NOT NULL,
                    contracts INTEGER NOT NULL,
                    redis_stream_id VARCHAR(50) NOT NULL
                )
            """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orderbook_deltas (
                    timestamp BIGINT NOT NULL,
                    ticker VARCHAR(50) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    price_dollars DECIMAL(5, 4) NOT NULL,
                    delta INTEGER NOT NULL,
                    redis_stream_id VARCHAR(50) NOT NULL
                )
            """
            )
            self._conn.commit()

    def insert_orderbook_snapshots(self, records_df: pl.DataFrame):
        records_df.write_database(
            table_name="orderbook_snapshots",
            connection=self._database_url,
            if_table_exists="append",
            engine="adbc",
        )

    def insert_orderbook_deltas(self, records_df: pl.DataFrame):
        records_df.write_database(
            table_name="orderbook_deltas",
            connection=self._database_url,
            if_table_exists="append",
            engine="adbc",
        )
