from imp import reload
from hitbtc import HitBTC
import model

import pandas as pd
import fastparquet
import requests
import torch
import json


class Trader(HitBTC):
    def __init__(self, key, secret, pair):
        self.pair = pair
        super(Trader, self).__init__(key, secret)
        self.auth()
        self.send('subscribeTrades', {'symbol': pair}, 0)
        self.lstm = model.get_model()
        self.num_correct = 1
        self.num_incorrect = 0
        self.next_prediction = None
        self.last_value = None
        self.error_diff_long = 0
        self.error_diff_short = 0
        self.confidence = 0
        self.stats = []

    
    def handle_messages(self, message):
        if message.get('method', '') != 'updateTrades':
            return
        
        print(f'correct: {self.num_correct}, incorrect: {self.num_incorrect}: {self.num_correct * 100 / (self.num_incorrect + self.num_correct)}%, error long: {self.error_diff_long}, error short: {self.error_diff_short}, last confidence: {self.confidence}')
        new_value = float(self.messages['updateTrades'][-1]['params']['data'][0]['price'])
        if self.next_prediction is not None and self.last_value is not None:
            if new_value > self.last_value:
                if self.next_prediction == 1:
                    self.num_correct += 1
                    self.stats.append(({'prediction': 1, 'correct': True, 'confidence': self.confidence}))
                else:
                    self.num_incorrect += 1
                    self.stats.append(({'prediction': 1, 'correct': False, 'confidence': self.confidence}))
                    self.error_diff_long += abs(new_value - self.last_value)
            elif new_value < self.last_value:
                if self.next_prediction == 0:
                    self.num_correct += 1
                    self.stats.append(({'prediction': 0, 'correct': True, 'confidence': self.confidence}))
                else:
                    self.num_incorrect += 1
                    self.stats.append(({'prediction': 0, 'correct': False, 'confidence': self.confidence}))
                    self.error_diff_short += abs(new_value - self.last_value)
            self.confidence = 0
            self.next_prediction = None
            df = pd.DataFrame(self.stats)
            fastparquet.write(f'stats/stats-{self.pair}.parquet', df, write_index=False)
        self.last_value = new_value
            
        try:
            df = pd.DataFrame([a for b in [m['params']['data'] for m in self.messages['updateTrades']] for a in b])
            df = df.assign(price=lambda x: pd.to_numeric(x.price)) \
                .assign(relative_returns=lambda x: (x.price - x.price.shift(1)) / x.price) \
                .assign(tick_returns=lambda x: (x.price - x.price.shift(1)) * 100)
            df = df[df.relative_returns.abs() > 0.]
            if len(df) > 9:
                df = df[-10:]
                inputs = torch.from_numpy(pd.np.expand_dims(df.relative_returns.values, 1)[None]).float()
                predictions = self.lstm(inputs)
                prediction = torch.max(predictions, 1)[1][0].item()
                confidence = torch.max(predictions, 1)[0].item()
                if confidence > 0.6:
                    self.confidence = confidence
                    self.next_prediction = prediction
                    print(f'confidence: {confidence}, prediction: {prediction}')
        except Exception as e:
            print(e)
            
            
key = open('key').read()
secret = open('secret').read()
pairs = list(pd.DataFrame(requests.get('https://api.hitbtc.com/api/2/public/symbol').json()).id)
traders = [Trader(key, secret, p) for p in pairs]
_ = [t.join() for t in traders]