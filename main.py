import asyncio
import base64
import json
import time
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import os
from dotenv import load_dotenv
from rich import print
import csv
import os

load_dotenv(override=True)

# Configuration
KEY_ID = os.getenv("KALSHI_API_KEY")
MARKET_TICKER = "KXNFLGAME-25NOV06LVDEN-DEN"  # Replace with any open market
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    
def process_snapshot_messsage(message: dict):
    timestamp = int(time.time() * 1000)  # milliseconds since epoch
    ticker = message.get('market_ticker')
    yes_dollars = message.get('yes_dollars', [])
    no_dollars = message.get('no_dollars', [])

    # Check if file exists to determine if we need to write headers
    file_exists = os.path.exists('snapshots.csv')

    with open('snapshots.csv', 'a', newline='') as file:
        writer = csv.writer(file)

        # Write header if file is new
        if not file_exists:
            writer.writerow(['timestamp', 'ticker', 'side', 'dollar', 'contracts'])

        # Write yes side data
        for dollar, contracts in yes_dollars:
            writer.writerow([timestamp, ticker, 'yes', dollar, contracts])

        # Write no side data
        for dollar, contracts in no_dollars:
            writer.writerow([timestamp, ticker, 'no', dollar, contracts])

def process_delta_messsage(message: dict):
    timestamp = int(time.time() * 1000)  # milliseconds since epoch
    ticker = message.get('market_ticker')
    side = message.get('side')
    dollar = message.get('price_dollars')
    delta = message.get('delta')

    # Check if file exists to determine if we need to write headers
    file_exists = os.path.exists('deltas.csv')

    with open('deltas.csv', 'a', newline='') as file:
        writer = csv.writer(file)

        # Write header if file is new
        if not file_exists:
            writer.writerow(['timestamp', 'ticker', 'side', 'dollar', 'delta'])

        writer.writerow([timestamp, ticker, side, dollar, delta])

def sign_pss_text(private_key: str, text: str) -> str:
    """Sign message using RSA-PSS"""
    message = text.encode('utf-8')
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')

def create_headers(private_key, method: str, path: str) -> dict:
    """Create authentication headers"""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split('?')[0]
    signature = sign_pss_text(private_key, msg_string)

    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }

def conv_payload_to_orderstruct(payload: dict) -> dict:
    """Convert orderbook payload to structured format"""
    orderbook = {
        "yes": [],
        "no": []
    }
    for price_level in payload.get("yes", []):
        orderbook["yes"].append({"price": price_level[0], "size": price_level[1]})
    for price_level in payload.get("no", []):
        orderbook["no"].append({"price": price_level[0], "size": price_level[1]})
    return orderbook

async def orderbook_websocket():
    """Connect to WebSocket and subscribe to orderbook"""
    # Load private key
    private_key_str = os.getenv("KALSHI_PRIVATE_KEY")
    private_key = serialization.load_pem_private_key(
        private_key_str.encode('utf-8'), password=None
    )

    # Create WebSocket headers
    ws_headers = create_headers(private_key, "GET", "/trade-api/ws/v2")

    async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
        print(f"Connected! Subscribing to orderbook for {MARKET_TICKER}")

        # Subscribe to orderbook
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_ticker": MARKET_TICKER
            }
        }
        await websocket.send(json.dumps(subscribe_msg))

        # Process messages
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "subscribed":
                print(f"Subscribed: {data}")

            elif msg_type == "orderbook_snapshot":
                process_snapshot_messsage(data.get('msg'))
                # print(f"Orderbook snapshot: {data}")

            elif msg_type == "orderbook_delta":
                # The client_order_id field is optional - only present when you caused the change
                if 'client_order_id' in data.get('data', {}):
                    print(f"Orderbook update (your order {data['data']['client_order_id']}): {data}")
                else:
                    process_delta_messsage(data.get('msg'))
                    # print(f"Orderbook update: {data}")

            elif msg_type == "error":
                print(f"Error: {data}")

            
        #   Snapshot and delta messages
        #  Orderbook snapshot (example of yes bids on the DEN side): 
        # {'type': 'orderbook_snapshot', 'sid': 1, 'seq': 1, 'msg':
        # {'market_ticker': 'KXNFLGAME-25NOV06LVDEN-DEN', 'market_id':
        # '095374b4-584c-48e1-a634-5e66787c39c5', 'yes': [[1, 961000], [2, 4800], [3,
        # 87847], [4, 3125], [5, 44294], [6, 2083], [7, 43728], [8, 2966], [9, 2055],
        # [10, 2200], [11, 115352], [12, 3308], [13, 3306], [14, 3302], [15, 39022], [16,
        # 3298], [17, 3296], [18, 3294], [19, 3293], [20, 3292], [21, 20555], [22, 3289],
        # [23, 3289], [24, 3287], [25, 3287], [26, 3781], [27, 3267], [28, 3267], [29,
        # 3267], [30, 3267], [31, 3306], [32, 2334], [33, 2334], [34, 2334], [35, 2334],
        # [36, 2334], [37, 1758], [38, 1401], [39, 1400], [40, 1450], [41, 1400], [42,
        # 1400], [43, 1400], [44, 1114], [45, 1114], [46, 1542], [47, 933], [48, 933],
        # [49, 933], [50, 1134], [51, 933], [52, 370], [53, 370], [54, 371], [55, 371],
        # [56, 370], [57, 370], [58, 370], [59, 370], [60, 632], [61, 282], [62, 143],
        # [63, 142], [65, 486], [67, 50], [68, 8], [69, 51], [70, 198], [71, 50], [72,
        # 60], [73, 115], [74, 1450], [75, 912], [76, 353], [77, 113], [78, 849596], [79,
        # 201986], [80, 778466]], 'no': [[1, 1558074], [2, 70199], [3, 90123], [4,
        # 18060], [5, 23884], [6, 17131], [7, 27249], [8, 3897], [9, 1066], [10, 312],
        # [11, 36], [12, 5], [13, 1], [15, 1], [16, 9496], [17, 346995], [18, 1086781],
        # [19, 5487283]], 'yes_dollars': [['0.0100', 961000], ['0.0200', 4800],
        # ['0.0300', 87847], ['0.0400', 3125], ['0.0500', 44294], ['0.0600', 2083],
        # ['0.0700', 43728], ['0.0800', 2966], ['0.0900', 2055], ['0.1000', 2200],
        # ['0.1100', 115352], ['0.1200', 3308], ['0.1300', 3306], ['0.1400', 3302],
        # ['0.1500', 39022], ['0.1600', 3298], ['0.1700', 3296], ['0.1800', 3294],
        # ['0.1900', 3293], ['0.2000', 3292], ['0.2100', 20555], ['0.2200', 3289],
        # ['0.2300', 3289], ['0.2400', 3287], ['0.2500', 3287], ['0.2600', 3781],
        # ['0.2700', 3267], ['0.2800', 3267], ['0.2900', 3267], ['0.3000', 3267],
        # ['0.3100', 3306], ['0.3200', 2334], ['0.3300', 2334], ['0.3400', 2334],
        # ['0.3500', 2334], ['0.3600', 2334], ['0.3700', 1758], ['0.3800', 1401],
        # ['0.3900', 1400], ['0.4000', 1450], ['0.4100', 1400], ['0.4200', 1400],
        # ['0.4300', 1400], ['0.4400', 1114], ['0.4500', 1114], ['0.4600', 1542],
        # ['0.4700', 933], ['0.4800', 933], ['0.4900', 933], ['0.5000', 1134], ['0.5100',
        # 933], ['0.5200', 370], ['0.5300', 370], ['0.5400', 371], ['0.5500', 371],
        # ['0.5600', 370], ['0.5700', 370], ['0.5800', 370], ['0.5900', 370], ['0.6000',
        # 632], ['0.6100', 282], ['0.6200', 143], ['0.6300', 142], ['0.6500', 486],
        # ['0.6700', 50], ['0.6800', 8], ['0.6900', 51], ['0.7000', 198], ['0.7100', 50],
        # ['0.7200', 60], ['0.7300', 115], ['0.7400', 1450], ['0.7500', 912], ['0.7600',
        # 353], ['0.7700', 113], ['0.7800', 849596], ['0.7900', 201986], ['0.8000',
        # 778466]], 'no_dollars': [['0.0100', 1558074], ['0.0200', 70199], ['0.0300',
        # 90123], ['0.0400', 18060], ['0.0500', 23884], ['0.0600', 17131], ['0.0700',
        # 27249], ['0.0800', 3897], ['0.0900', 1066], ['0.1000', 312], ['0.1100', 36],
        # ['0.1200', 5], ['0.1300', 1], ['0.1500', 1], ['0.1600', 9496], ['0.1700',
        # 346995], ['0.1800', 1086781], ['0.1900', 5487283]]}}

# Run the example
if __name__ == "__main__":
    asyncio.run(orderbook_websocket())