import requests
import json
import numpy as np
import pandas as pd
from datetime import datetime
import pymysql
import coinconfig as cfg

class paprika():
  ratesAPI = "rates"
  db_host = ""
  db_username = ""
  db_password = ""
  db_database = ""
  db_connection = ""
  db_cursor = ""
  cm_apiBase = "https://api.coinmotion.com/v2/"
  cm_ratesAPI = "rates"
  

  def __init__(self, name="", debug=False):
    self.loadConfig()
    self.df =pd.DataFrame
    self.name = name
    self.datasetSQL  = "select * from (select * from paprika where coin='"+self.tickers[self.name]+"' order by time_open desc limit 10080)Var1 order by id asc"

  def loadConfig(self):
    print("\nLoading Config...")

    self.db_host = cfg.config["host"]
    self.db_username = cfg.config["username"]
    self.db_password = cfg.config["password"]
    self.db_database = cfg.config["database"]
    #self.MA = cfg.config["MA"]
    self.buyBelowMA = float(cfg.config["buyLimit"])
    #print("Buy Limit " + str(self.buyBelowMA * 100) + "%")
    #self.sellAboveBuyPrice = float(cfg.config["sellLimit"])
    #print("Sell Limit " + str(self.sellAboveBuyPrice * 100) + "%")

    self.tickers = cfg.tickers
    self.cm_tickers = cfg.cm_tickers

  def connectDB(self):
    self.db_connection = pymysql.connect(host=self.db_host, user=self.db_username, password=self.db_password, database=self.db_database)
    self.db_cursor = self.db_connection.cursor()

  def closeDB(self):
    self.db_connection.close()

  def initDF(self):
    try:
      self.connectDB()
      SQL_Query = pd.read_sql_query(self.datasetSQL, self.db_connection)
      self.df = pd.DataFrame(SQL_Query, columns=['id','time_open','time_close','openn','high','low','close','volume','market_cap'])
      #df = self.df.loc[::-1].reset_index(drop = True) # reverse order
      self.closeDB()
    except Exception as e:
      print(e)
    
  def addMovingAverages(self):

    # cumulative moving average
    self.df['cma'] = self.df.close.expanding().mean()

    # exponential ma
    self.df['ema12'] = self.df.close.ewm(span=12, adjust=False).mean()
    self.df['ema26'] = self.df.close.ewm(span=26, adjust=False).mean()

    # simple ma
    self.df['sma20'] = self.df.close.rolling(20, min_periods=1).mean()
    self.df['sma50'] = self.df.close.rolling(50, min_periods=1).mean()
    self.df['sma200'] = self.df.close.rolling(200, min_periods=1).mean()

  def addMomentumIndicators(self):
    """Appends RSI14 and MACD momentum indicators to a dataframe."""

    if not isinstance(self.df, pd.DataFrame):
        raise TypeError('Pandas DataFrame required.')

    if not 'close' in self.df.columns:
        raise AttributeError("Pandas DataFrame 'close' column required.")

    if not self.df['close'].dtype == 'float64' and not self.df['close'].dtype == 'int64':
        raise AttributeError("Pandas DataFrame 'close' column not int64 or float64.")

    if not 'ema12' in self.df.columns:
      self.df['ema12'] = self.df.close.ewm(span=12, adjust=False).mean()

    if not 'ema26' in self.df.columns:
        self.df['ema26'] = self.df.close.ewm(span=26, adjust=False).mean()

    if not self.df['ema12'].dtype == 'float64' and not self.df['ema12'].dtype == 'int64':
        raise AttributeError("Pandas DataFrame 'ema12' column not int64 or float64.")

    if not self.df['ema26'].dtype == 'float64' and not self.df['ema26'].dtype == 'int64':
      raise AttributeError("Pandas DataFrame 'ema26' column not int64 or float64.")

    # calculate relative strength index
    self.df['rsi14'] = self.calculateRelativeStrengthIndex(self.df['close'], 14)
    
    # default to midway-50 for first entries
    self.df['rsi14'] = self.df['rsi14'].fillna(50)

    # calculate moving average convergence divergence
    self.df['macd'] = self.df['ema12'] - self.df['ema26']
    self.df['signal'] = self.df['macd'].ewm(span=9, adjust=False).mean()

    # calculate on-balance volume (obv)
    self.df['obv'] = np.where(self.df['close'] > self.df['close'].shift(1), self.df['volume'], 
      np.where(self.df['close'] < self.df['close'].shift(1), -self.df['volume'], self.df.iloc[0]['volume'])).cumsum()

    # obv change percentage
    self.df['obv_pc'] = self.df['obv'].pct_change() * 100
    self.df['obv_pc'] = np.round(self.df['obv_pc'].fillna(0), 2)

  def calculateRelativeStrengthIndex(self, series, interval=14):
    """Calculates the RSI on a Pandas series of closing prices."""

    if not isinstance(series, pd.Series):
        raise TypeError('Pandas Series required.')

    if not isinstance(interval, int):
        raise TypeError('Interval integer required.')

    if(len(series) < interval):
        raise IndexError('Pandas Series smaller than interval.')

    diff = series.diff(1).dropna()

    sum_gains = 0 * diff
    sum_gains[diff > 0] = diff[diff > 0]
    avg_gains = sum_gains.ewm(com=interval-1, min_periods=interval).mean()

    sum_losses = 0 * diff
    sum_losses[diff < 0] = diff[diff < 0]
    avg_losses = sum_losses.ewm(
      com=interval-1, min_periods=interval).mean()

    rs = abs(avg_gains / avg_losses)
    rsi = 100 - 100 / (1 + rs)

    return rsi

  def getPrice(self):
    try:
        request = requests.get(
            url = self.cm_apiBase+self.cm_ratesAPI
        )
        parsed = json.loads(request.content)

        ts = datetime.fromtimestamp(parsed['payload']['timestamp'])
        
        buy = parsed['payload'][self.cm_tickers[self.name]]['buy']
        sell = parsed['payload'][self.cm_tickers[self.name]]['sell']

        return ts, buy, sell
    
    except requests.exceptions.RequestException as e:
        print(e)

