import websocket
import hashlib
import hmac
import json
import time
import os
import pandas as pd
import database
import indicators as ind



class Socket:
    def __init__(self):
        self.update = False
        self.m = Manager()
        pass

#FUNCTIONS WEBSOCKET
    def on_message(self, message):
        data = json.loads(message)
        ti = time.asctime(time.localtime(time.time()))
        info = {'time_event': data['data']['E'],
            'time_open_candle': data['data']['k']['t'],
            'time_close_candle': data['data']['k']['T'],
            'open': data['data']['k']['o'],
            'high': data['data']['k']['h'],
            'low': data['data']['k']['l'],
            'close': data['data']['k']['c'],
            'volume': data['data']['k']['v'],
            'closed': data['data']['k']['x']}

        if info['closed'] == True:
            self.db.c.execute("INSERT INTO dados(time_event, time_open_candle, time_close_candle, open, high, low, close, volume, closed) "
                              "VALUES("+str(info['time_event'])+","+str(info['time_open_candle'])+","+str(info['time_close_candle'])+","+str(info['open'])+","
                              +str(info['high'])+","+str(info['low'])+","+str(info['close'])+","+str(info['volume'])+","+str(info['closed'])+")")
            self.db.conn.commit()
            self.update = True
            if self.update == True:
                data = self.get_data_database()
                self.m._put_data_on_manager(data, ti)

    def on_error(self, error):
        print(error)


    def on_close(self):
        print('close websocket')
        os._exit(0)

    def on_open(self):
        sub_ticker = {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@kline_1m"],
        "id": 1
         }
        self.ws.send(json.dumps(sub_ticker))

    def connect_api(self):
        websocket.enableTrace(True)
        self.db = database.DataBase('database.db')
        self.db.create_table()
        self.ws = websocket.WebSocketApp('wss://stream.binance.com:9443/stream?streams=',
                            on_message = self.on_message,
                            on_error = self.on_error,
                            on_close = self.on_close,
                            on_open = self.on_open)
        self.ws.run_forever()

    def get_data_database(self):
        self.db.c.execute("SELECT * FROM dados ORDER BY time_event DESC LIMIT 1")
        resul = self.db.c.fetchone()
        self.db.conn.commit()
        return resul

class Manager:
    def __init__(self):
        self.data = None
        self.dataframe = pd.DataFrame(columns=['time_event', 'time_open_candle', 'time_close_candle',
                                               'open', 'high', 'low', 'close', 'volume', 'closed'])

        self.df = pd.DataFrame(columns=['time_event', 'time_open_candle', 'time_close_candle',
                                               'open', 'high', 'low', 'close', 'volume', 'closed'])
        self.e = Estrategia()
    def _manager(self):
        pass

    def _put_data_on_manager(self, data, ti):
        tin = ti
        self.data = data
        df = pd.DataFrame([self.data], columns=['time_event', 'time_open_candle', 'time_close_candle',
                                                'open', 'high', 'low', 'close', 'volume', 'closed'])

        if not self.dataframe.empty:
            frames = [self.dataframe, df]
            self.dataframe = pd.concat(frames, ignore_index=True)
            self.e.get(self.send_data_from_manager(), tin=tin)
        else:
            frames = [self.df, df]
            self.dataframe = pd.concat(frames, ignore_index=True)
            self.e.get(self.send_data_from_manager(), tin=tin)

    def send_data_from_manager(self):
        return self.dataframe

class Estrategia:
    def __init__(self):
        self.emaf = 2
        self.emas = 4

    def get(self, data, tin):
        dataframe = data
        ind.EMA(df=dataframe, base='close', target='emaf', period=self.emaf)
        ind.EMA(df=dataframe, base='close', target='emas', period=self.emas)
        if len(dataframe) < 5:
            pass
        else:
            self.strategy(dataframe=dataframe)

    def strategy(self, dataframe):
        emaf_actual = dataframe.iloc[-1:,9]
        emas_actual = dataframe.iloc[-1:,10]
        emaf_ = dataframe.iloc[-2:,9]
        emas_ = dataframe.iloc[-2:,10]
        emaf_previus = emaf_[:-1]
        emas_previus = emas_[:-1]
        diff_emaf = float(emaf_actual) - float(emaf_previus)
        diff_emas = float(emas_actual) - float(emas_previus)


        if float(emaf_previus) < float(emas_previus) and float(emaf_actual) > float(emas_actual):
            pass
        elif float(emaf_previus) > float(emas_actual) and float(emaf_actual) < float(emas_actual):
            pass


if __name__=='__main__':
    a = Socket()
    a.connect_api()
