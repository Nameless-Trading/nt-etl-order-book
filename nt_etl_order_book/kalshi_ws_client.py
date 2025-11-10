import asyncio
import base64
import csv
import json
import os
import time

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv
from rich import print

    


class KalshiWSClient:

    def __init__(
        self,
        kalshi_api_key: str | None = None,
        ws_url: str | None = None,
        private_key: str | None = None,
    ) -> None:
        load_dotenv(override=True)

        if kalshi_api_key is None:
            self._kalshi_api_key = os.getenv("KALSHI_API_KEY")
        
        if ws_url is None:
            self._ws_url = os.getenv("KALSHI_WS_URL")

        if private_key is None:
            self._private_key = os.getenv("KALSHI_PRIVATE_KEY")


    @staticmethod
    def _process_snapshot_message(message: dict):
        timestamp = int(time.time() * 1000)  # milliseconds since epoch
        ticker = message.get("market_ticker")
        yes_dollars = message.get("yes_dollars", [])
        no_dollars = message.get("no_dollars", [])

        # Check if file exists to determine if we need to write headers
        file_exists = os.path.exists("testing_snapshots.csv")

        with open("testing_snapshots.csv", "a", newline="") as file:
            writer = csv.writer(file)

            # Write header if file is new
            if not file_exists:
                writer.writerow(["timestamp", "ticker", "side", "dollar", "contracts"])

            # Write yes side data
            for dollar, contracts in yes_dollars:
                writer.writerow([timestamp, ticker, "yes", dollar, contracts])

            # Write no side data
            for dollar, contracts in no_dollars:
                writer.writerow([timestamp, ticker, "no", dollar, contracts])

    @staticmethod
    def _process_delta_message(message: dict):
        timestamp = int(time.time() * 1000)  # milliseconds since epoch
        ticker = message.get("market_ticker")
        side = message.get("side")
        dollar = message.get("price_dollars")
        delta = message.get("delta")

        # Check if file exists to determine if we need to write headers
        file_exists = os.path.exists("testing_deltas.csv")

        with open("testing_deltas.csv", "a", newline="") as file:
            writer = csv.writer(file)

            # Write header if file is new
            if not file_exists:
                writer.writerow(["timestamp", "ticker", "side", "dollar", "delta"])

            writer.writerow([timestamp, ticker, side, dollar, delta])


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


    async def get_order_book_messages(self, market_tickers: list[str]):
        """Connect to WebSocket and subscribe to orderbook"""
        # Create WebSocket headers
        ws_headers = self._create_headers("GET", "/trade-api/ws/v2")

        async with websockets.connect(self._ws_url, additional_headers=ws_headers) as websocket:
            print(f"Connected! Subscribing to orderbook.")

            # Subscribe to orderbook
            subscribe_msg = {
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": ["orderbook_delta"], "market_tickers": market_tickers},
            }
            await websocket.send(json.dumps(subscribe_msg))

            # Initialize sequence tracking
            expected_seq = 1

            # Check message for valid seq
            async for message in websocket:
                data = json.loads(message)
                msg_type = data['type']

                # Validate sequence number if present
                if msg_type in ['orderbook_snapshot', 'orderbook_delta']:
                    seq = data['seq']

                    if seq != expected_seq:
                        raise RuntimeError(f"Missed message! Expected seq: {expected_seq}, Received seq: {seq}")
                    else:
                        expected_seq += 1
                
                yield data

if __name__ == '__main__':
    async def main():
        market_tickers = ["KXNCAAFGAME-25NOV15TEXUGA-UGA", "KXNCAAFGAME-25NOV15TEXUGA-TEX"]
        kalshi_client = KalshiWSClient()
        await kalshi_client.get_order_book_messages(market_tickers=market_tickers)

    asyncio.run(main())