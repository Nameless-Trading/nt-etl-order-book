import asyncio

from kalshi_rest_client import KalshiRestClient
from kalshi_ws_client import KalshiWSClient
from redis_client import RedisClient


class Producer:
    def __init__(self):
        self.kalshi_rest_client = KalshiRestClient()
        self.kalshi_ws_client = KalshiWSClient()
        self.redis_client = RedisClient()

    async def _save_with_error_handling(self, coro):
        """Helper to save to Redis with error handling (fire-and-forget)."""
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

                # Save to Redis based on message type (fire-and-forget with error handling)
                if msg_type == "orderbook_snapshot":
                    asyncio.create_task(
                        self._save_with_error_handling(
                            self.redis_client.save_orderbook_snapshot(message)
                        )
                    )

                elif msg_type == "orderbook_delta":
                    asyncio.create_task(
                        self._save_with_error_handling(
                            self.redis_client.save_orderbook_delta(message)
                        )
                    )

                else:
                    # For other message types (like 'subscribed', 'error'), just print
                    print(f"Received {msg_type}: {message}")
        finally:
            # Clean up Redis connection
            await self.redis_client.close()
