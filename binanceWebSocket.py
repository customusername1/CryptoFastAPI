import asyncio
import websockets
import json
import datetime
from websockets.exceptions import ConnectionClosed

class BinanceWebsocket:
    
    def __init__(self, exchanges_data_list):
        self.url = "wss://stream.binance.com:9443/ws/!ticker@arr"
        self.binance_assets = set()
        self.binance_assets_data = exchanges_data_list
        '''
        Binance have !ticker@arr which response for all current pairs with have updates. No need request any pairs before or subscribe for all pairs one by one.
        '''
    async def connect(self):
        try:                
            async with websockets.connect(self.url) as websocket:
                while True:
                    response = await websocket.recv()
                    ticker_data = json.loads(response)

                    for ticker in ticker_data:
                        #This save all assets from response in set() for saving unique assets list. Than it sending it into assets dictionary -> self.binance_assets['assets']
                        self.binance_assets.add(ticker['s'])

                        for data in self.binance_assets:
                            #This send availible assets from respons on assets dictionary and save all possible values from each response
                            self.binance_assets_data['assets'][data] = {
                                'pair':data
                            }
                        tss = int(datetime.datetime.now().timestamp() * 1000)    
                        average_price = (float(ticker["a"])+float(ticker["b"])/2)
                        self.binance_assets_data['price'][ticker['s']] = {
                            'ask':ticker['a'],
                            'bid':ticker['b'],
                            'average':average_price,
                            'tss':tss
                        }
                        self.binance_assets_data['last_update'] = tss
        except ConnectionClosed:
            return self.connect()
        #Next update will include exception and lost connections.


if __name__ == '__main__':
    exchanges_data_list = {'binance': {'assets': {},'price': {}, 'last_update':{}}}
    manager = BinanceWebsocket(exchanges_data_list['binance'])

    async def main():
        await manager.connect()
    asyncio.run(main())

