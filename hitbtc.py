from threading import Thread
import pandas as pd
import websocket
import logging
import queue
import time
import json

websocket_url = 'wss://api.hitbtc.com/api/2/ws'
logger = logging.getLogger('HITBTC')
logger.setLevel(logging.INFO)


class HitBTC(Thread):
    def __init__(self, key, secret, max_size=15):
        self.key = key
        self.secret = secret
        self.ws = websocket.WebSocketApp(websocket_url, on_open=self.on_open, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        self.max_size = max_size
        self.messages = []
        self.connected = False
        super(HitBTC, self).__init__(target=self.ws.run_forever, args=[])
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
        if len(self.messages) == self.max_size:
            self.messages.pop(0)
        self.messages.append(message)
        self.handle_messages(message)

    def on_error(self, ws, error):
        print(f'on_error: {error}')

    def on_close(self, ws, *args):
        print(f'on_close: {args}')
        
    def handle_messages(self, message):
        pass
    

