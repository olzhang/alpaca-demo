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

os.environ["APCA_API_KEY_ID"] = "AK369OPHSF98F2J623N7"
os.environ["APCA_API_SECRET_KEY"] = "ajb8sATuQQjJUwKMcVK829o4j5UAidin4spC7t/9"

IEX_BASE_URL = "https://api.iextrading.com/1.0"

NY = "America/New_York"

class Algo:

    def __init__(self, done=TOMORROW):
        logger.info("instantiating class")
        self.universe = self.load_universe()
        self.done = done
        self.api = tradeapi.REST()


    def load_universe(self):
        logger.info("loading universe")
        path_to_data = os.path.join(os.path.dirname(__file__), './data/constituents.csv')
        return pd.read_csv(path_to_data)

    def prices(self, symbols):
        """
        get prices from list of symbols
        """
        logger.info("getting prices")
        now = pd.Timestamp.now(tz=NY)
        end_dt = now

        if end_dt.time() >= pd.Timestamp("09:30", tz=NY).time():
            end_dt = now - pd.Timedelta(now.strftime("%H:%M:%S")) - pd.Timedelta("1 minute")
        return self._get_polygon_prices(symbols, end_dt)

    def _get_iex_prices(self, symbols, end_dt):
        logger.info("calling alpaca data api to get iex stats on {} symbols for 30 days".format(str(len(symbols))))

        from_date = (end_dt - pd.Timedelta("30 days")).strftime("%Y-%-m-%-d")
        to_date = end_dt.strftime("%Y-%-m-%-d")

        results = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:

            future_to_symbol = {
                executor.submit(
                    self.__historic_polygon_query,
                    symbol,
                    from_date,
                    to_date): symbol for symbol in symbols
            }

            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    results[symbol] = future.result()
                except Exception as e:
                    logger.warning("{} has error {}".format(str(symbol), str(e)))
        
        return results


    def _iex_request(self, url):


    def main():
        while True:

            clock = self.api.get_clock()
            now = clock.timestamp

            if clock.is_open and done != now.strftime("%Y-%m-%d"):

                # execute some trades
                done = now.strftime("%Y-%m-%d")
                logger.info("finished executing tick at {}".format(done))

            time.sleep(1)


if __name__ == "__main__":
    algo = Algo()
    sp500_symbols = algo.universe.head()["Symbol"].tolist()
    for k, v in algo.prices(sp500_symbols):
        print("{} : {}".format(str(k), v))

