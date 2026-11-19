import datetime
import enum
import json
import logging
import random
import re
import string
import pandas as pd
from websocket import create_connection
import requests
import json

logger = logging.getLogger(__name__)


class Interval(enum.Enum):
    min_1 = "1"
    min_3 = "3"
    min_5 = "5"
    min_15 = "15"
    min_30 = "30"
    min_45 = "45"
    hour_1 = "1H"
    hour_2 = "2H"
    hour_3 = "3H"
    hour_4 = "4H"
    daily = "1D"
    weekly = "1W"
    monthly = "1M"


class TradingViewData:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    # TradingView balances its data websocket across several hosts. Some
    # networks/DNS resolvers only expose a subset of them, so fall back to the
    # other hosts when the first connection cannot be established.
    __ws_hosts = (
        "data.tradingview.com",
        "prodata.tradingview.com",
        "widgetdata.tradingview.com",
    )
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __search_headers = {
        'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'),
        'Origin': 'https://www.tradingview.com',
        'Referer': 'https://www.tradingview.com/',
        'Accept': 'application/json',
    }
    __ws_timeout = 15

    def __init__(
        self,
        username: str = None,
        password: str = None,
    ) -> None:

        self.ws_debug = False

        self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"


        self.ws = None
        self.__ws_host = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):

        if (username is None or password is None):
            token = None

        else:
            data = {"username": username,
                    "password": password,
                    "remember": "on"}
            try:
                response = requests.post(
                    url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                token = response.json()['user']['auth_token']
            except Exception as e:
                logger.error('error while signin')
                token = None

        return token

    def __create_connection(self):
        logging.debug("creating websocket connection")
        last_error = None

        hosts = list(self.__ws_hosts)
        if self.__ws_host:
            # Prefer the host that worked last time so a broken DNS entry is
            # not retried for every request.
            hosts.remove(self.__ws_host)
            hosts.insert(0, self.__ws_host)

        for host in hosts:
            url = f"wss://{host}/socket.io/websocket"
            try:
                self.ws = create_connection(
                    url, headers=self.__ws_headers, timeout=self.__ws_timeout
                )
                self.__ws_host = host
                logger.debug("connected to %s", host)
                return
            except Exception as e:
                last_error = e
                logger.debug("could not connect to %s: %s", host, e)
                self.ws = None

        raise ConnectionError(
            "unable to connect to the TradingView websocket"
        ) from last_error

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            print(m)
        self.ws.send(m)

    @staticmethod
    def __contains_chart_error(result, chart_session):
        """Return True when a chart/series message reports an error."""
        for part in result.split("~m~"):
            if not part or part.isdigit():
                continue
            try:
                message = json.loads(part)
            except ValueError:
                continue
            if not isinstance(message, dict):
                continue
            if message.get("m") not in (
                "symbol_error",
                "series_error",
                "critical_error",
                "protocol_error",
            ):
                continue
            params = message.get("p") or []
            if params and params[0] == chart_session:
                return True
        return False

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            out = re.search(r'"s":\[(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = re.split(r"\[|:|,|\]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]))

                row = [ts]

                for i in range(5, 10):

                    # skip converting volume data if does not exists
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))

                    except ValueError:
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            data.insert(0, "symbol", value=symbol)
            return data
        except (AttributeError, IndexError, ValueError):
            logger.error("no data, please check the exchange and symbol")

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
    ) -> pd.DataFrame:
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "symbol_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        self.__send_message(
            "create_series",
            [self.chart_session, "s1", "s1", "symbol_1", interval, n_bars],
        )
        self.__send_message("switch_timezone", [
                            self.chart_session, "exchange"])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        try:
            while True:
                try:
                    result = self.ws.recv()
                except Exception as e:
                    logger.error(e)
                    break

                # TradingView sends heartbeat frames that must be echoed back,
                # otherwise the server eventually drops the connection.
                for heartbeat in re.findall(r"~m~\d+~m~~h~\d+", result):
                    self.ws.send(heartbeat)

                raw_data = raw_data + result + "\n"

                if "series_completed" in result:
                    break

                # Invalid symbols or symbols that need a paid subscription
                # never send series_completed, so stop instead of waiting for
                # the socket to time out. Only treat errors for the chart
                # session as fatal.
                if self.__contains_chart_error(
                    result, self.chart_session
                ):
                    logger.error(
                        "TradingView returned an error for %s", symbol
                    )
                    break
        finally:
            if self.ws is not None:
                try:
                    self.ws.close()
                except Exception:
                    pass
                self.ws = None

        return self.__create_df(raw_data, symbol)

    def search(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)

        symbols_list = []
        try:
            resp = requests.get(
                url, headers=self.__search_headers, timeout=self.__ws_timeout
            )
            resp.raise_for_status()

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    tv = TradingViewData()
    print(tv.get_hist("CRUDEOIL", "MCX", fut_contract=1))
    print(tv.get_hist("NIFTY", "NSE", fut_contract=1))
    print(
        tv.get_hist(
            "EICHERMOT",
            "NSE",
            interval=Interval.hour_1,
            n_bars=500,
            extended_session=False,
        )
    )
