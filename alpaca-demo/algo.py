import alpaca_trade_api as tradeapi
import pandas as pd
import os
import logging
from logging.config import fileConfig
from datetime import timedelta, datetime
import time
import concurrent
import requests

fileConfig("./logging_config.ini")
logger = logging.getLogger(__file__)

TOMORROW = datetime.now() + timedelta(days=1)

os.environ["APCA_API_KEY_ID"] = "PK3VJ3DGTT989D8AE45L"
os.environ["APCA_API_SECRET_KEY"] = "NmMBrOwibFXo1n4by7MXb7ujhVN5QHkDEU0Q8Aau"

NY = "America/New_York"

class Algo:

    def __init__(self, done=TOMORROW):
        logger.info("instantiating class")
        
        self.universe = None
        self.symbols = []
        self.df_barset = None
        self.scores = []
        self.orders = []

        self.max_risk_diff = -0.1
        self.position_size = 150 # dollar value of a position of stock (# symbol * $ per symbol)
        self.max_positions = 100 # how many symbols do we hold?
        self.since = 3

        self.end_dt = pd.Timestamp.now(tz=NY)
        self.interval = "5Min"
        self.limit_bars = min(5*4, 100)

        self.done = done
        self.api = tradeapi.REST(base_url="https://paper-api.alpaca.markets")
        self.account = self.api.get_account()

        self.load_universe()
        self.set_barsets()
        self.set_scores()

    def get_df_barset(self):
        return self.df_barset

    def get_symbols(self):
        return self.symbols

    def get_universe(self):
        return self.universe

    def get_scores(self):
        return self.scores
    
    def get_api(self):
        return self.api
    
    def get_account(self):
        return self.account
    
    def get_current_orders(self):
        return self.orders

    def set_position_size(self, size):
        self.position_size = size
        logger.info("position_size now: {}".format(self.position_size))

    def set_max_positions(self, size):
        self.set_max_positions = size
        logger.info("max_position size now: {}".format(self.max_positions))

    def set_limit_bars(self, limit_bars):
        self.limit_bars = limit_bars

    def set_interval(self, interval):
        self.interval = interval

    def set_end_dt(self, end_dt):
        self.end_dt = end_dt

    def set_since(self, since):
        self.since = since
    
    def load_universe(self):
        logger.info("loading universe")
        path_to_data = os.path.join(os.path.dirname(__file__), './data/constituents.csv')
        self.universe = pd.read_csv(path_to_data)
        self.symbols = self.universe["Symbol"].tolist()

    def set_barsets(self):
        """
        get prices from list of symbols
        """
        logger.info("setting barsets ...")

        date_to_query = self.end_dt

        if self.end_dt.time() >= pd.Timestamp("09:30", tz=NY).time():
            date_to_query = self.end_dt - pd.Timedelta(self.end_dt.strftime("%H:%M:%S")) - pd.Timedelta("1 minute")

        self._build_iex_df_barset(date_to_query)

    def __query_iex_barset(self, symbols, from_date, to_date):
        logger.debug("calling alpaca barset to get iex stats on symbols {} from {} to {}".format(str(symbols), str(from_date), str(to_date)))
        
        r = requests.get("https://data.alpaca.markets/v1/bars/{}".format(self.interval), \
            params={"symbols": ",".join(symbols), "limit": self.limit_bars, "start": from_date, "end": to_date}, \
            headers={"APCA-API-KEY-ID": os.environ["APCA_API_KEY_ID"], "APCA-API-SECRET-KEY": os.environ["APCA_API_SECRET_KEY"]})
        
        logger.debug("request made to: {}".format(r.url))
        logger.debug("request returned with status of {}".format(str(r.status_code)))
        return r.json()

    def _build_iex_df_barset(self, date_to_query):
        logger.debug("calling alpaca data api to get iex stats on {} symbols for {} hours".format(str(len(self.symbols)), self.since))

        from_date = (date_to_query - pd.Timedelta("{} hours".format(self.since))).strftime("%Y-%m-%d")
        to_date = date_to_query.strftime("%Y-%m-%d")

        # queries 200 of them at the same time
        logger.debug("requesting for the first 200 symbols")
        barset = self.__query_iex_barset(self.symbols[0:200], from_date=from_date, to_date=to_date)
        for x in range(200, len(self.symbols), 200):
            logger.debug("requesting for the next {} symbols".format(str(x)))
            barset.update(self.__query_iex_barset(self.symbols[x:x+200], from_date=from_date, to_date=to_date))
        
        data = {'ticker': [], 't': [], 'o': [], 'h': [], 'l': [], 'c': [], 'v': []}
        for k, v in barset.items():
            for entry in v:
                data['ticker'].append(k)
            for name in ['t', 'o', 'h', 'l', 'c', 'v']:
                for entry in v:
                    value = entry.get(name)
                    if name == 't':
                        value = pd.Timestamp(value, unit="s").strftime("%Y-%m-%d %H:%M:%S")
                    data[name].append(value)
        df_barset = pd.DataFrame(data)
        tuple_col_headers = list(zip(df_barset.ticker.tolist(),df_barset.t.tolist()))
        multi_headers = pd.MultiIndex.from_tuples(tuple_col_headers)
        del df_barset["ticker"]
        del df_barset["t"]
        df_barset.index = multi_headers
        self.df_barset = df_barset

    def set_scores(self):
        diffs = {}
        param = 10
        for symbol in self.df_barset.index.levels[0]:
            df = self.df_barset.xs(str(symbol))
            if len(df.c.values) <= param:
                logger.debug("{} has {} values less than {}".format(symbol, str(len(df.c.values), param)))
                continue
            ema = df.c.ewm(span=param).mean()[-1]
            last = df.c.values[-1]
            diff = (last - ema) / last
            logger.debug("{} closed at {} on {} ema: {} diff: {} ".format(symbol, df.c.values[-1], self.df_barset.iloc[-1].name[1], str(ema), str(diff)))
            
            if diff < self.max_risk_diff:
                logger.warning("{} below max_risk_diff: {}".format(symbol, self.max_risk_diff))
                continue
            
            diffs[symbol] = diff

        self.scores = sorted(diffs.items(), key=lambda x: x[1])
        
    def set_orders(self):
        to_buy = []
        to_buy_symbols = set()

        top = len(self.scores) // 20

        # only look at top 5% of the stocks and exclude those that are too expensive
        for symbol, score in self.scores[0:top]:
            last_price = self.df_barset.xs(symbol).c.values[-1]
            if float(last_price) > float(self.account.cash):
                logger.info("price of {} closed {} is larger than all {} cash in account".format(symbol, last_price, self.account.cash))
                continue
            to_buy.append((symbol, score))
            to_buy_symbols.add(symbol)
        logger.info("to_buy: {}".format(to_buy))

        # get existing portfolio
        positions = self.api.list_positions()
        holdings = {p.symbol: p for p in positions}
        current_position_symbols = set(holdings.keys())
        logger.info("current_position_symbols: {}".format({p.symbol: p.qty for p in positions}))
        # sell all the ones that are not in to_buy
        to_sell_symbols = current_position_symbols - to_buy_symbols
        logger.info("to_sell_symbols: {}".format(to_sell_symbols))
        # take out the ones that we already have
        to_buy_symbols = to_buy_symbols - current_position_symbols
        logger.info("to_buy_symbols less current_position_symbols: {}".format(to_buy_symbols))

        to_buy = [e for e in to_buy if e[0] in to_buy_symbols]
        logger.info("to_buy: {}".format(to_buy))

        orders = []

        # build sell orders ...
        for symbol in to_sell_symbols:
            sell_order = { "symbol": symbol, "qty": holdings[symbol].qty, "side": "sell" }
            logger.info("orders to sell are: {}".format(sell_order))
            orders.append(sell_order)

        # build buy orders ...
        max_positions_to_buy = self.max_positions - (len(current_position_symbols) - len(to_sell_symbols))
        
        buying_power = self.account.buying_power
        for symbol, _ in to_buy:
            if max_positions_to_buy < 1:
                logger.info("max_positions_to_buy: {} count is less than 1".format(str(max_positions_to_buy)))
                break
            shares_to_buy = self.position_size // float(self.df_barset.xs(symbol).c.values[-1])
            if shares_to_buy == 0:
                logger.info("price of 1 share of {} is larger than total position size {}".format(symbol, self.position_size))
                continue
            buy_order = { "symbol": symbol, "qty": shares_to_buy, "side": "buy"}
            logger.info("buy order: {}".format(buy_order))
            orders.append(buy_order)
            max_positions_to_buy = max_positions_to_buy - 1

        self.orders = orders

    def submit_order(self, side, wait=0, order_type="market"):
        # process the sell orders first
        curr_orders = [o for o in self.orders if o["side"] == side]
        logger.info("curr_orders: {}".format(curr_orders))
        for order in curr_orders:
            try:
                logger.info("submit({}): {}".format(side, order))
                self.api.submit_order(symbol=order['symbol'], qty=order['qty'], side=side, type=order_type, time_in_force='day')
            except Exception as e:
                logger.error("encountered error: {}".format(e))

        count = wait
        while count > 0:
            pending = self.api.list_orders()
            if len(pending) == 0:
                logger.info("all {} orders done".format(side))
                break
            logger.info("{} {} orders pending...".format(len(pending), side))
            time.sleep(1)
            count -= 1

    def trade(self):
        self.submit_order("sell", wait=300)
        self.submit_order("buy", wait=300)


if __name__ == "__main__":

    
    logger.info("start execution")
    
    alg = Algo()

    while True:
        # clock API returns the server time including
        # the boolean flag for market open
        clock = alg.get_api().get_clock()
        now = clock.timestamp
        
        logger.debug("clock.is_open: {}".format(clock.is_open))
        # logger.debug("now: {}".format(now))
        if clock.is_open:

            alg.set_barsets()
            
            alg.set_scores()
            
            alg.set_orders()

            alg.trade()

            done = now.strftime('%Y-%m-%d %H:%M:%S')
            logger.info("done for {}".format(done))

            time.sleep(3600 * 3)
        else:
            logger.info("sleeping for now .....")
            time.sleep(3600)

