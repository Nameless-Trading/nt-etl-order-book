import base64
import os
import time

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv


class KalshiRestClient:
    def __init__(
        self,
        kalshi_api_key: str | None = None,
        base_url: str | None = None,
        private_key: str | None = None,
    ) -> None:
        load_dotenv(override=True)

        if kalshi_api_key is None:
            self._kalshi_api_key = os.getenv("KALSHI_API_KEY")

        if base_url is None:
            self._base_url = os.getenv("KALSHI_BASE_URL")

        if private_key is None:
            self._private_key = os.getenv("KALSHI_PRIVATE_KEY")

    @staticmethod
    def _sign_pss_text(private_key, text: str) -> str:
        """Sign message using RSA-PSS"""
        message = text.encode("utf-8")
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _create_headers(self, method: str, path: str) -> dict:
        """Create authentication headers"""
        # Load private key
        private_key = serialization.load_pem_private_key(
            self._private_key.encode("utf-8"), password=None
        )

        timestamp = str(int(time.time() * 1000))
        msg_string = timestamp + method + path.split("?")[0]
        signature = self._sign_pss_text(private_key, msg_string)

        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self._kalshi_api_key,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }

    def get_tickers(self, series_ticker: str) -> list[str]:
        endpoint = "/trade-api/v2/markets"
        limit = 1000

        url = self._base_url + endpoint
        headers = self._create_headers("GET", path=endpoint)

        params = {"series_ticker": series_ticker, "limit": limit}

        response = requests.get(url=url, params=params, headers=headers)
        markets = response.json()["markets"]
        tickers = [market["ticker"] for market in markets]

        return tickers
