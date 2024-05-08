import os

import requests
import time
import logging
import datetime

api_key = os.environ.get("API_KEY")

logging.basicConfig(level=logging.INFO)


def get_dividend_announcements(the_day: datetime.datetime):
    uri_template = "https://api.polygon.io/v3/reference/dividends?ex_dividend_date.gte={date}&limit=500&order=asc&sort=ex_dividend_date&apiKey={apikey}"

    repeat = True
    size = 0
    date = (the_day + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    uri = uri_template.format(apikey=api_key, date=date)
    while repeat:
        try:
            r = requests.get(uri)
            r.raise_for_status()
            r = r.json()
            if size > 0:
                time.sleep(60 / 5)
            if "next_url" in r:
                uri = r["next_url"] + "&apiKey=" + api_key
                repeat = True
            else:
                repeat = False
            if "results" in r:
                size += len(r["results"])
                logging.info(
                    f"size: {size}, ex_dividend_date: {r['results'][-1]['ex_dividend_date']}"
                )
                for asset in r["results"]:
                    yield asset
        except requests.exceptions.HTTPError as err:
            logging.info(err)
            time.sleep(30)
            repeat = True
        except Exception as e:
            logging.error(repr(e))
            repeat = False
