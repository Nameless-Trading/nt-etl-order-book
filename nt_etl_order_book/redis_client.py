import redis.asyncio
from dotenv import load_dotenv
import os

class RedisClient:

    def __init__(self, redis_public_url: str | None = None):
        load_dotenv(override=True)

        if redis_public_url is None:
            self._redis_public_url = os.getenv("REDIS_PUBLIC_URL")

        self._client = redis.asyncio.from_url(self._redis_public_url)

if __name__ == '__main__':
    redis_client = RedisClient()
