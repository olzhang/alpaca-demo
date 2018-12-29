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

NY = "America/New_York"

class Algo:

    def __init__(self, done=TOMORROW):
        logger.info("instantiating class")
        self.universe = self.load_universe()
        self.done = done
        self.api = tradeapi.REST(base_url="https://paper-api.alpaca.markets")


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
        return self._get_iex_prices(symbols, end_dt)

    def __get_iex_barset_prices(self, symbols, from_date, to_date, limit=50, interval="day"):
        logging.info("calling alpaca barset to get iex stats on symbols {} from {} to {}".format(str(symbols), str(from_date), str(to_date)))
        r = requests.get("https://data.alpaca.markets/v1/bars/{}".format(interval), \
            params={"symbols": ",".join(symbols), "limit": limit, "start": from_date, "end": to_date}, \
            headers={"APCA_API_KEY_ID": os.environ["APCA_API_KEY_ID"], "APCA_API_SECRET_KEY": os.environ["APCA_API_SECRET_KEY"]})
        logger.info("request made to: {}".format(r.url))
        logger.info("request returned with status of {}".format(str(r.status_code)))
        return r.json()

    def _get_iex_prices(self, symbols, end_dt):
        logger.info("calling alpaca data api to get iex stats on {} symbols for 50 days".format(str(len(symbols))))

        from_date = (end_dt - pd.Timedelta("50 days")).strftime("%Y-%m-%d")
        to_date = end_dt.strftime("%Y-%m-%d")

        # queries 200 of them at the same time
        logger.info("requesting for the first 200 symbols")
        barset = self.__get_iex_barset_prices(symbols[0:200], from_date=from_date, to_date=to_date)
        for x in range(200, len(symbols), 200):
            logger.info("requesting for the next {} symbols".format(str(x)))
            barset.update(self.__get_iex_barset_prices(symbols[x:x+200], from_date=from_date, to_date=to_date))
        return barset

    def main(self):
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
    sp500_symbols = algo.universe["Symbol"].tolist()
    print(algo.prices(sp500_symbols))
