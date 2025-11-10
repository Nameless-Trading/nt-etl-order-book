from kalshi_rest_client import KalshiRestClient
from kalshi_ws_client import KalshiWSClient
from redis_client import RedisClient


class Producer:
    def __init__(self):
        self.kalshi_rest_client = KalshiRestClient()
        self.kalshi_ws_client = KalshiWSClient()
        self.redis_client = RedisClient()

    async def run(self, series_ticker: str) -> None:
        market_tickers = self.kalshi_rest_client.get_tickers(
            series_ticker=series_ticker
        )

        # TESTING!
        market_tickers = ['KXNCAAFGAME-25NOV15TEXUGA-UGA']

        try:
            async for message in self.kalshi_ws_client.get_order_book_messages(
                market_tickers=market_tickers
            ):
                msg_type = message.get("type")

                # Save to Redis based on message type
                if msg_type == "orderbook_snapshot":
                    message_id = await self.redis_client.save_orderbook_snapshot(
                        message
                    )
                    print(f"Saved snapshot to Redis: {message_id}")

                elif msg_type == "orderbook_delta":
                    message_id = await self.redis_client.save_orderbook_delta(message)
                    print(f"Saved delta to Redis: {message_id}")

                else:
                    # For other message types (like 'subscribed', 'error'), just print
                    print(f"Received {msg_type}: {message}")
        finally:
            # Clean up Redis connection
            await self.redis_client.close()
