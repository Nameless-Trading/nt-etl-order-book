import asyncio

from kalshi_rest_client import KalshiRestClient
from kalshi_ws_client import KalshiWSClient
from redis_client import RedisClient


class Producer:
    def __init__(self):
        self.kalshi_rest_client = KalshiRestClient()
        self.kalshi_ws_client = KalshiWSClient()
        self.redis_client = RedisClient()
        self.pending_tasks = set()

    async def _save_with_error_handling(self, coro):
        """Helper to save to Redis with error handling."""
        try:
            message_id = await coro
            print(f"Saved to Redis: {message_id}")
        except Exception as e:
            print(f"Error saving to Redis: {e}")

    async def run(self, series_ticker: str) -> None:
        market_tickers = self.kalshi_rest_client.get_tickers(
            series_ticker=series_ticker
        )

        # # TESTING!
        # market_tickers = ['KXNCAAFGAME-25NOV15TEXUGA-UGA']

        try:
            async for message in self.kalshi_ws_client.get_order_book_messages(
                market_tickers=market_tickers
            ):
                msg_type = message.get("type")

                # Save to Redis based on message type
                if msg_type == "orderbook_snapshot":
                    task = asyncio.create_task(
                        self._save_with_error_handling(
                            self.redis_client.save_orderbook_snapshot(message)
                        )
                    )
                    self.pending_tasks.add(task)
                    task.add_done_callback(self.pending_tasks.discard)

                elif msg_type == "orderbook_delta":
                    task = asyncio.create_task(
                        self._save_with_error_handling(
                            self.redis_client.save_orderbook_delta(message)
                        )
                    )
                    self.pending_tasks.add(task)
                    task.add_done_callback(self.pending_tasks.discard)

                else:
                    # For other message types (like 'subscribed', 'error'), just print
                    print(f"Received {msg_type}: {message}")
        finally:
            # Wait for all pending tasks to complete before closing
            if self.pending_tasks:
                print(f"Waiting for {len(self.pending_tasks)} pending Redis writes to complete...")
                await asyncio.gather(*self.pending_tasks, return_exceptions=True)

            # Clean up Redis connection
            await self.redis_client.close()
