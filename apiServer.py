import asyncio
import uvicorn
import datetime

from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel

from krakenWebSocket import KrakenWebSocket
from binanceWebSocket import BinanceWebsocket
from kuCoinWebSocket import KuCoinWebsocket
from huobiWebSocket import HuobiWebSocket



app = FastAPI()

exchanges = {
    'kraken': KrakenWebSocket,
    'binance': BinanceWebsocket,
    'kucoin': KuCoinWebsocket,
    'huobi': HuobiWebSocket
}

exchanges_data_list = {
    'kraken': {'assets': {},'price': {}, 'last_update':{}}, 
    'binance': {'assets': {},'price': {}, 'last_update':{}},
    'kucoin':{'assets': {},'price': {}, 'last_update':{}},
    'huobi':{'assets': {},'price': {}, 'last_update':{}}
    }

kraken_ws  = KrakenWebSocket(exchanges_data_list['kraken'])
binance_ws = BinanceWebsocket(exchanges_data_list['binance'])
kucoin_ws = KuCoinWebsocket(exchanges_data_list['kucoin'])
huobi_ws = HuobiWebSocket(exchanges_data_list['huobi'])

@app.on_event('startup')
async def startup_event():
    asyncio.create_task(kraken_ws.connect())
    asyncio.create_task(binance_ws.connect())
    asyncio.create_task(kucoin_ws.connect())
    asyncio.create_task(huobi_ws.connect())

'''
###### GET METHOD ######
'''

@app.get('/')
async def start_info():
    start_info = {
        'status': 'online',
        'tss':int(datetime.datetime.now().timestamp() * 1000), 
        'exchanges': [exchange for exchange in exchanges_data_list],
        'requestsLinks': {
            '/allData/':{
                'params':{
                    'pair':'str',
                    'exchange':'str',
                    'assets':'bool'
                }
            },
            '/price/':{
                'params':{
                    'pair':'str',
                    'exchange':'str',
                }
            },
            '/assets':{
                'params':{
                    'exchange':'str',
                }
            },
        },
        'allowedMethods':{
            '1':'GET',
            '2':'POST'
        },
    }
    return start_info

@app.get('/allData/')
async def get_all_data(pair: str = None, exchange: str = None, assets: bool = False):
    tss = int(datetime.datetime.now().timestamp() * 1000)
    response = await data_response(pair=pair, exchange=exchange, assets=assets)

    if not exchange:
        exchange = 'all'

    if not pair:
        pair = 'all'

    if response:
        return {'status':'Success',
                'tss': tss,
                'request':{
                    'assets':assets,
                    'pair':pair.lower(),
                    'exchange':exchange.lower()
                },
                'result':response
                }   
    return {'status':'error',
                'tss': tss,
                'request':{
                    'exchange(-s)':exchange,
                    'pair': pair,
                    }
                }   

@app.get('/price/')
async def get_all_price(pair: str = None, exchange: str = None):
    tss = int(datetime.datetime.now().timestamp() * 1000)
    response = await data_response(pair=pair, exchange=exchange, assets=False)

    if not exchange:
        exchange = 'all'

    if not pair:
        pair = 'all'

    if response:
        return {'status':'Success',
                'tss': tss,
                'request':{
                    'pair':pair,
                    'exchange':exchange.lower()
                },
                'result':response
                }
    
    else:
        return {'status':'error',
                'tss': tss,
                'request':{
                    'assets': None,
                    'exchange(-s)':f'Exchange: {exchange} or Pair: {pair} => incorrect',
                    'example': [exchange for exchange in exchanges_data_list]
                    }
                }     
     
@app.get('/assets/')
async def get_all_assets(exchange: str = None): 
    tss = int(datetime.datetime.now().timestamp() * 1000)
    response = await get_assets()   
    if not exchange:
        return {'status':'Success',
                'tss': tss,
                'request':{
                    'assets':'all',
                    'exchanges':[exchange for exchange in exchanges_data_list]
                },
                'result':response
                }
    
    elif exchange.lower() in exchanges_data_list:
        return {
            'status':'Success',
            'tss': tss,
            'request':{
                'assetsQuery': 'specified',
                'exchange':exchange.lower(),
                },
            'result':response
            }    
       
    else:
        return {'status':'error',
                'tss': tss,
                'request':{
                    'assets': None,
                    'exchange(-s)':f'{exchange.lower()} => incorrect',
                    'example': [exchange for exchange in exchanges_data_list]
                    }
                }     

'''
###### POST METHOD #####
'''

@app.post('/allData/')
async def post_all_data(pair: str = None, exchange: str = None, assets: bool = False):
    tss = int(datetime.datetime.now().timestamp() * 1000)
    response = await data_response(pair=pair, exchange=exchange, assets=assets)

    if not exchange:
        exchange = 'all'

    if not pair:
        pair = 'all'

    if response:
        return {'status':'Success',
                'tss': tss,
                'request':{
                    'assets':assets,
                    'pair':pair.upper(),
                    'exchange':exchange.lower()
                },
                'result':response
                }   
    return {'status':'error',
                'tss': tss,
                'request':{
                    'exchange(-s)':exchange,
                    'pair': pair,
                    }
                }   

@app.post('/price/')
async def post_all_price(pair: str = None, exchange: str = None):
    tss = int(datetime.datetime.now().timestamp() * 1000)
    response = await data_response(pair=pair, exchange=exchange, assets=False)

    if not exchange:
        exchange = 'all'

    if not pair:
        pair = 'all'

    if response:
        return {'status':'Success',
                'tss': tss,
                'request':{
                    'pair':pair.upper(),
                    'exchange':exchange.lower()
                },
                'result':response
                }
    
    else:
        return {'status':'error',
                'tss': tss,
                'request':{
                    'assets': None,
                    'exchange(-s)':f'Exchange: {exchange} or Pair: {pair} => incorrect',
                    'example': [exchange for exchange in exchanges_data_list]
                    }
                }     
     
@app.post('/assets/')
async def post_all_assets(exchange: str = None): 
    tss = int(datetime.datetime.now().timestamp() * 1000)
    response = await get_assets()   
    if not exchange:
        return {'status':'Success',
                'tss': tss,
                'request':{
                    'assets':'all',
                    'exchanges':[exchange for exchange in exchanges_data_list]
                },
                'result':response
                }
    
    elif exchange.lower() in exchanges_data_list:
        return {
            'status':'Success',
            'tss': tss,
            'request':{
                'assetsQuery': 'specified',
                'exchange':exchange.lower(),
                },
            'result':response
            }    
       
    else:
        return {'status':'error',
                'tss': tss,
                'request':{
                    'assets': None,
                    'exchange(-s)':f'{exchange.lower()} => incorrect',
                    'example': [exchange for exchange in exchanges_data_list]
                    }
                }

'''
###### DATA PROCESSING ######
'''

async def get_assets():
    result = {}

    for exch_name, exch_data in exchanges_data_list.items():
            assets = exch_data['assets']
            result[exch_name] = {
                    'assetsNumber': len(assets),
                    'assetsData':assets
                }
            
    return result 

async def data_response(assets, pair, exchange):
    result = {}
    empty_result = {"kraken": {},"binance": {},"kucoin": {},"huobi": {}}
    
    #If received empty request parameters
    if not pair and not exchange: 
        if assets == True: #If False, then a list with data on trading pairs name 'Assets' isn't sent in response
            return exchanges_data_list
        for exch_name, exch_data in exchanges_data_list.items():
            try:
                result[exch_name] = {
                    'priceList':exch_data['price']}
            except KeyError:
                result = False
        return result
    
    #For request with specified only Pair, Exchange == None
    elif pair and not exchange:
        result = {}
        for exch_name, exch_data in exchanges_data_list.items():
            try:
                result[exch_name] = {}
                if assets == True: #If False, then a list with data on trading pairs name 'Assets' isn't sent in response
                    for ws_pair, pair_name in exch_data['pairs'].items():
                        if pair.upper() == pair_name['pair']:
                            result[exch_name]['asstes'] = {
                                ws_pair: pair_name['pair']
                            }
                else:
                    for price_name, price_data in exch_data['price'].items():
                        if pair.upper() == price_name:
                            result[exch_name]['price'] = {
                                'price': {
                                    price_name: price_data
                                }
                            }
            except KeyError:
                pass  
        if result == empty_result: #Check if the result is not an empty dictionary of exchanges names
            result = False
        return result
    
    #For request with specified only Exchange, Pair == None    
    elif exchange and not pair:
        try:
            for exch_name, exch_data  in exchanges_data_list.items():
                if exch_name == exchange.lower():
                    if assets == True: #If False, then a list with data on trading pairs name 'Assets' isn't sent in response
                        result[exchange] = exch_data
                        return result

                    result[exchange] = {'price':exch_data['price']}
                    return result
        except KeyError:
            result = False
            return result        
           
    #For request with specified Pair and Exchange
    elif pair and exchange:
        try:
            for exch_name, exch_data in exchanges_data_list.items():
                if exch_name == exchange.lower():
                    result[exch_name] = {}
                    if assets == True: #If False, then a list with data on trading pairs name 'Assets' isn't sent in response
                        for pair_name, pair_data in exch_data['assets'].items():
                            if pair.upper() == pair_data['pair']:
                                result[exch_name]['pair'] = {
                                    'wsName': pair_name,
                                    'requestName': pair_data['pair']
                                }
                    else:
                        for price_name, price_data in exch_data['price'].items(): #Checks all pairs on the specified exchange
                            if pair.upper() == price_name:
                                result[exch_name]['price'] = {
                                    'pair': price_name,
                                    'price_data': price_data,
                                }
        except KeyError:
            result = False
    if not result[exchange.lower()]: #If no pair in specified exchange, result return {exchange: {}}, its checks if result empty dict for error, or not 
        result = False
    return result


if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=8000)