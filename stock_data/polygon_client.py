import os

import requests
import time
import logging
import datetime

api_key = os.environ.get("API_KEY")

logging.basicConfig(level=logging.INFO)


def get_dividend_announcements(symbol: str, the_day: datetime.date):
    uri_template = "https://api.polygon.io/v3/reference/dividends?ticker={symbol}&ex_dividend_date.gte={date}&limit=1000&order=asc&sort=ex_dividend_date&apiKey={apikey}"

    repeat = True
    size = 0
    date = (the_day + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    uri = uri_template.format(apikey=api_key, date=date, symbol=symbol)
    while repeat:
        try:
            r = requests.get(uri, timeout=(3, 10))
            r.raise_for_status()
            r = r.json()
            if "next_url" in r:
                uri = r["next_url"] + "&apiKey=" + api_key
                repeat = True
            else:
                repeat = False
            if "results" in r:
                size += len(r["results"])
                if r["results"]:
                    for asset in r["results"]:
                        if "currency" in asset and asset["currency"] == "USD":
                            yield asset
                        elif "currency" not in asset:
                            yield asset
                    logging.info(
                        f"size: {size}, ex_dividend_date: {r['results'][-1]['ex_dividend_date']}"
                    )

        except requests.exceptions.HTTPError as err:
            if r.status_code != 429:
                logging.error(err)
                raise err
            logging.info(err)
            time.sleep(60)
            repeat = True
        except Exception as e:
            logging.error(repr(e))
            repeat = False


def ticker_info(symbol):
    uri_template = (
        "https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={apikey}"
    )
    uri = uri_template.format(symbol=symbol, apikey=api_key)
    r = requests.get(uri, timeout=(3, 10))
    r.raise_for_status()
    json = r.json()
    if "results" in json:
        return json["results"]
    return json
