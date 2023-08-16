import json
import asyncio
import websockets
import requests
import gzip
import datetime

from websockets.exceptions import ConnectionClosed

class HuobiWebSocket():
    def __init__(self, exchanges_data_list):
        self.assets_set = set()
        self.assets_list = exchanges_data_list

    async def connect(self):
        await self.get_assets_list()
        try:
            await self.web_socket()
        except ConnectionClosed:
            return self.connect()

    async def web_socket(self):
        url = 'wss://api-aws.huobi.pro/ws'
        async with websockets.connect(url) as websocket:
            for asset in self.assets_set:
                subscribe_request = {
                    "sub": f"market.{asset}.ticker"
                }
                await websocket.send(json.dumps(subscribe_request))
            while True:
                await self.message_recv(websocket=websocket)
                    

    async def message_recv(self, websocket):
        message = await websocket.recv()
        load_message = json.loads(gzip.decompress(message).decode('utf-8'))
        tss = int(datetime.datetime.now().timestamp() * 1000)
        if 'ping' in load_message:
            await websocket.send(json.dumps({'pong':load_message['ping']}))

        elif 'ch' in load_message:
            name = load_message['ch'].replace('market.', '').replace('.ticker', '').upper()
            averageAksBid = float(f'{(load_message["tick"]["ask"]+load_message["tick"]["bid"])/2}')
            self.assets_list['price'][name] = {
                'ask':load_message['tick']['ask'],
                'bid':load_message['tick']['bid'],
                'lastPrice':load_message['tick']['lastPrice'],
                'averageAskBid': averageAksBid,
                'ts': tss
                }
        else:
            pass
        self.assets_list['last_update'] = tss

    async def get_assets_list(self):
        '''Huobi WebSocket require REST API request for symbols. 
        Huobi haven't connection to all trading pairs or ticker, which response with trading symbols for Websocket Market Data subscribe.
        Need to create a subscription request for each trading pair
        '''
        url = 'https://api.huobi.pro/v1/common/symbols'
        response = requests.get(url).json()
        for asset in response['data']:
            if asset['state'] == 'online':
                try:
                    name = asset['symbol']
                    self.assets_set.add(name)
                    self.assets_list['assets'][name.upper()] = {
                        'pair':name.upper()
                    }
                except:
                    pass

if __name__ == '__main__':
    exchanges_data_list = {'huobi': {'assets': {},'price': {}, 'last_update':{}}}
    manager = HuobiWebSocket(exchanges_data_list['huobi'])
    asyncio.run(manager.connect())