import asyncio

from consumer import Consumer
from producer import Producer

async def main():
    series_ticker = "KXNCAAFGAME"
    producer = Producer()
    consumer = Consumer()

    # Run producer and consumer concurrently
    await asyncio.gather(
        producer.run(series_ticker=series_ticker),
        consumer.run()
    )

if __name__ == "__main__":
    asyncio.run(main())
