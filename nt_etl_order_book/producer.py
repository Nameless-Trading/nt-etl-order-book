from kalshi_rest_client import KalshiRestClient
from kalshi_ws_client import KalshiWSClient
from redis_client import RedisClient
import asyncio

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
        market_tickers = ["KXNCAAFGAME-25NOV15TEXUGA-UGA"] #, "KXNCAAFGAME-25NOV15TEXUGA-TEX"]

        async for message in self.kalshi_ws_client.get_order_book_messages(
            market_tickers=market_tickers
        ):
            print(message)


if __name__ == "__main__":
    from rich import print
    series_ticker = "KXNCAAFGAME"
    producer = Producer()
    asyncio.run(producer.run(series_ticker=series_ticker))
