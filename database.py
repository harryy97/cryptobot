import sqlite3

class DataBase:
    def __init__(self, nome):
        self.name = nome
        self.conn = sqlite3.connect(self.name)
        self.c = self.conn.cursor()

    def create_table(self):
        self.c.execute('CREATE TABLE IF NOT EXISTS dados (time_event timestamp, time_open_candle timestamp, time_close_candle timestamp, '
                       'open float, high float, low float,close float, volume float, closed bool)')


