import websockets
import json
import asyncio
import datetime

from websockets.exceptions import ConnectionClosed

class KrakenWebSocket:

    def __init__(self, exchanges_data_list):
        self.url = 'wss://ws.kraken.com/v2'
        self.websocket = None
        self.connection_list = []
        self.assets_payload = {
            "method": "subscribe",
            "params": {
                "channel": "instrument",
                "snapshot": True
            },
            "req_id": 18321216463246013119
            }
        self.pairs_data = exchanges_data_list
        self.assets_payload_list = []

    '''
    KuCoin include wss channel "instrument" for retrieving all traging pairs on the exchange
    Before starting connection with main KuCoin websocket for loading market data with price for all trading pairs, we collect primary information about all assets
    '''
    async def get_assets_list(self):
        async with websockets.connect(self.url) as ws_assets:
            self.websocket = ws_assets  
            await ws_assets.send(json.dumps(self.assets_payload))

            if self.websocket is None:
                await self.get_assets_list()
            while True:
                recv_message = await self.websocket.recv()
                self.load_message = json.loads(recv_message)
                
                try:
                    if 'instrument' in self.load_message ['channel']:
                        ws_pairs = self.load_message['data']['pairs']
                        assets_list = self.pairs_data['assets'] = {}
                        for pair_name in ws_pairs:
                            self.pairs_data['assets'][pair_name['symbol']] = {
                                'pair': f"{pair_name['base']}{pair_name['quote']}"
                            }
                        await ws_assets.close()

                        return self.pairs_data
                except KeyError as ke:
                    pass
            
    
    async def connect(self):
        assets_list = await self.get_assets_list()
        try: 
            async with websockets.connect(self.url) as websocket:
                self.websocket = websocket  
                
                for ws_pair, pair in assets_list['assets'].items():
                    self.assets_payload_list.append(ws_pair)

                self.assets_price_payload = {
                    "method": "subscribe",
                    "params": {
                        "channel": "ticker",
                        "snapshot": True,
                        "symbol": self.assets_payload_list
                    },
                    "req_id": 1234567890
                    }
            
                await websocket.send(json.dumps(self.assets_price_payload))

                while True:
                    recv_assets = await self.websocket.recv()
                    assets_message = json.loads(recv_assets)

                    try:
                        if 'ticker' in  assets_message['channel']:
                            for assets_details in assets_message['data']:
                                tss = int(datetime.datetime.now().timestamp() * 1000)
                                pair_name = assets_details['symbol'].replace('/','')
                                ask_price = assets_details['ask']
                                bid_price = assets_details['ask']
                                self.pairs_data['price'][pair_name] = {
                                        'ask':ask_price,
                                        'bid':bid_price,
                                        'average':f'{float((ask_price+bid_price)/2)}',
                                        'tss':tss
                                    }
                                self.pairs_data['last_update'] = tss
                    except KeyError as ke:
                        pass
        except ConnectionClosed:
            return self.connect()

if __name__ == '__main__':
    exchanges_data_list = {'kraken': {'assets': {},'price': {}, 'last_update':{}}}
    manager = KrakenWebSocket(exchanges_data_list['kraken'])

    async def main():
        await manager.connect()

    asyncio.run(main())
