import asyncio
import websockets
import json

import requests
import datetime

from websockets.exceptions import ConnectionClosed

class KuCoinWebsocket:

    def __init__(self, exchanges_data_list):
        self.url = self.request_access_token()
        self.kukoin_assets = set()
        self.ws_token_access = ()
        self.kukoin_assets_data = exchanges_data_list
        self.kukoin_payload = {
            "id": 1545910660739,                          
            "type": "subscribe",
            "topic": "/market/ticker:all",
            "response": True                              
        }

    def request_access_token(self):
        '''
        KuCoin require access token from REST API empty post request for connection to Websocket
        '''
        request = requests.post('https://api.kucoin.com/api/v1/bullet-public').json()
        ws_token_access = request['data']['token']
        url = f"wss://ws-api-spot.kucoin.com/endpoint?token={ws_token_access}"
        return url

    async def connect(self): 
        if self.ws_token_access == ():
            self.request_access_token()
        try:
            async with websockets.connect(self.url) as websocket:
                await websocket.send(json.dumps(self.kukoin_payload))
                while True:
                    response = await websocket.recv()
                    ticker_data = json.loads(response)

                    if 'topic' in ticker_data:

                        self.kukoin_assets.add(ticker_data['subject'])
                        for data in self.kukoin_assets:
                                self.kukoin_assets_data['assets'][data] = {
                                    'pair':data.replace('-', '')
                                }
                        for _, _ in ticker_data['data'].items():
                            tss = int(datetime.datetime.now().timestamp() * 1000)
                            average_bid = (float(ticker_data['data']['bestAsk'])+float(ticker_data['data']['bestBid']))/2
                            self.kukoin_assets_data['price'][ticker_data['subject'].replace('-', '')] = {
                                'price':ticker_data['data']['price'],
                                'bestAsk':ticker_data['data']['bestAsk'],
                                'bestBid':ticker_data['data']['bestBid'],
                                'average_bid':average_bid,
                                'tss':tss
                            }
                            self.kukoin_assets_data['last_update'] = tss

        except:
            return self.connect()

                
if __name__ == '__main__':
    exchanges_data_list = {'kukoin': {'assets': {},'price': {}, 'last_update':{}}}
    manager = KuCoinWebsocket(exchanges_data_list['kukoin'])

    async def main():
        await manager.connect()
        
    asyncio.run(main())

