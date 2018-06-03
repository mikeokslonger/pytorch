from threading import Thread
import pandas as pd
import websocket
import requests
import logging
import queue
import time
import json

websocket_url = 'wss://api.hitbtc.com/api/2/ws'
sync_api_url = 'https://api.hitbtc.com'
logger = logging.getLogger('HITBTC')
logger.setLevel(logging.INFO)


class HitBTC(Thread):
    def __init__(self, key, secret, max_size=25, start=True):
        self.key = key
        self.secret = secret
        self.sync_session = requests.Session()
        self.sync_session.auth = (key, secret)
        self.ws = websocket.WebSocketApp(websocket_url, on_open=self.on_open, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        self.max_size = max_size
        self.messages = {}
        self.connected = False
        super(HitBTC, self).__init__(target=self.ws.run_forever, args=[])
        if start:
            self.start()
            self.auth()
        
    def start(self):
        super(HitBTC, self).start()
        while not self.connected:
            time.sleep(0.1)
        
    def auth(self):
        payload = {'method': 'login', 'params': {'sKey': self.secret, 'algo': 'BASIC', 'pKey': self.key}, 'id': int(10000 * time.time())}
        self.ws.send(json.dumps(payload))
        
    def send(self, method, params, id=None):
        payload = {'method': method, 'params': params, 'id': id or int(10000 * time.time())}
        self.ws.send(json.dumps(payload))
        
    def on_open(self, ws):
        self.connected = True
    
    def on_message(self, ws, message):
        parsed_message = json.loads(message)
        message_type = parsed_message.get('method', 'unknown')
        if message_type not in self.messages:
            self.messages[message_type] = []
        queue = self.messages[message_type]
        if len(queue) == self.max_size:
            queue.pop(0)
        queue.append(parsed_message)
        try:
            self.handle_messages(parsed_message)
        except Exception as e:
            logger.exception(e)

    def on_error(self, ws, error):
        print(f'on_error: {error}')
        super(HitBTC, self).__init__(self.key, self.secret, self.max_size)
        print('Restarted')

    def on_close(self, ws, *args):
        print(f'on_close: {args}')
        
    def handle_messages(self, message):
        pass
    

class OrderBook(HitBTC):
    def __init__(self, key, secret, pair):
        super(OrderBook, self).__init__(key, secret)
        self.send('subscribeOrderbook', {'symbol': pair})
        self.asks = None
        self.bids = None
        
    def handle_messages(self, message):
        if message.get('method', '') == 'snapshotOrderbook':
            self.asks = pd.DataFrame(message['params']['ask']) \
                .assign(price=lambda x: pd.to_numeric(x.price)) \
                .assign(size=lambda x: pd.to_numeric(x['size'])).set_index('price')
            self.bids = pd.DataFrame(message['params']['bid']) \
                .assign(price=lambda x: pd.to_numeric(x.price)) \
                .assign(size=lambda x: pd.to_numeric(x['size'])).set_index('price')
        elif message.get('method', '') == 'updateOrderbook':
            if message['params']['bid']:
                update_bids = pd.DataFrame(message['params']['bid']) \
                    .assign(price=lambda x: pd.to_numeric(x.price)) \
                    .assign(size=lambda x: pd.to_numeric(x['size'])).set_index('price')
                new_bids = pd.concat([self.bids[~self.bids.index.isin(update_bids.index)], update_bids]).sort_index(ascending=False)
                self.bids = new_bids[new_bids['size'] > 0]
            if message['params']['ask']:
                update_asks = pd.DataFrame(message['params']['ask']) \
                    .assign(price=lambda x: pd.to_numeric(x.price)) \
                    .assign(size=lambda x: pd.to_numeric(x['size'])).set_index('price')
                new_asks = pd.concat([self.asks[~self.asks.index.isin(update_asks.index)], update_asks]).sort_index()
                self.asks = new_asks[new_asks['size'] > 0]

        
    def show(self, n=3):
        display(self.asks.head(n=n))
        display(self.bids.head(n=n))