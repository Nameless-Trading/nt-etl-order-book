import asyncio

from consumer import Consumer

if __name__ == "__main__":
    # series_ticker = "KXNCAAFGAME"
    # producer = Producer()
    # asyncio.run(producer.run(series_ticker=series_ticker))

    consumer = Consumer()
    asyncio.run(consumer.run())
