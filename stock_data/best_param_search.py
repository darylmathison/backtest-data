import datetime
import logging
import multiprocessing

import stock_data.risk_reward as rr
import stock_data.fill_data as fd
import stock_data.models as model

stop_loss_percentages = [r / 100 for r in range(10, 16, 1)]


class DataLoadError(Exception):
    pass


class DividendMultiplierSearch:

    def __init__(self, start_date, end_date):
        self.checked = []
        self.next = None
        self.tune = False
        self.end_date = end_date
        self.start_date = start_date

    def pick_next(self, session, asset):
        num_of_rounds = len(self.checked)
        if num_of_rounds == 0:
            return 1
        elif num_of_rounds == 1:
            return 2
        elif num_of_rounds >= 2 and num_of_rounds < 15:
            risk_diff = get_risk_diffs(session, asset)
            if risk_diff[0] > 0 and self.tune is False:
                return self.next + 1
            elif risk_diff[0] <= 0 and self.tune is False:
                self.tune = True
                return self.next - 0.75
            elif risk_diff[0] > 0 and self.tune is True:
                return self.next + 0.25
            elif risk_diff[0] <= 0 and self.tune is True:
                return None
        return None

    def find(self, symbol):
        self.checked = []
        self.tune = False
        with fd.open_session() as session:
            asset = (
                session.query(model.Assets)
                .filter(model.Assets.symbol == symbol)
                .first()
            )
            self.next = self.pick_next(session, asset)
            while self.next is not None:
                self.checked.append(self.next)
                (
                    _win_rate,
                    loss_rate,
                    avg_gain,
                    avg_loss,
                    percentage_downloaded,
                    avg_dividend,
                    div_multiplier,
                    stop_loss_percentage,
                ) = rr.backtest_security(
                    session,
                    self.start_date,
                    self.end_date,
                    asset,
                    5,
                    self.next,
                    stop_loss_percentages[0],
                )
                if _win_rate is not None and (avg_loss > 0 and avg_gain > 0):
                    portion_to_risk = (_win_rate / avg_loss) - (loss_rate / avg_gain)
                    risk_entry = model.RiskReward(
                        symbol=asset.symbol,
                        win_rate=_win_rate,
                        avg_gain=avg_gain,
                        loss_rate=loss_rate,
                        avg_loss=avg_loss,
                        percentage_downloaded=percentage_downloaded,
                        avg_dividend=avg_dividend,
                        portion_to_risk=portion_to_risk,
                        last_update=datetime.datetime.now(),
                        div_multiplier=div_multiplier,
                        stop_loss_percentage=stop_loss_percentage,
                    )
                    session.add(risk_entry)
                    session.commit()
                else:
                    logging.warning(
                        f"Could not calculate risk reward for {asset.symbol} with multiplier {self.next}"
                    )
                    break
                self.next = self.pick_next(session, asset)


def risk_numbers(session, asset):
    return list(
        (
            session.query(model.RiskReward)
            .filter(model.RiskReward.symbol == asset.symbol)
            .order_by(model.RiskReward.last_update.desc())
            .all()
        )
    )


# @retry((DataLoadError,), max_retries=3, backoff=FixedBackOff(2))
def get_risk_diffs(session, asset):
    last_risk_to_reward = risk_numbers(session, asset)
    risk_diff = []
    for i in range(len(last_risk_to_reward) - 1):
        diff = (
            last_risk_to_reward[i].portion_to_risk
            - last_risk_to_reward[i + 1].portion_to_risk
        )
        # change the diff to the only 3 decimal places
        formatted_diff = round(diff, 3)
        risk_diff.append(formatted_diff)
    return risk_diff


def find(symbol, start, end):
    searcher = DividendMultiplierSearch(start, end)
    searcher.find(symbol)


def main():
    end = datetime.date.today()
    start = datetime.date(end.year - 10, end.month, end.day)
    # searcher = DividendMultiplierSearch(start, end)
    with fd.open_session() as session:
        symbols = {stock.symbol for stock in rr.dividend_stocks(session)}
        existing_evaluations = {
            symbol[0] for symbol in session.query(model.RiskReward.symbol).all()
        }
    with multiprocessing.Pool(7) as pool:
        symbols_left = [(s, start, end) for s in sorted(symbols - existing_evaluations)]
        pool.starmap(find, symbols_left)


if __name__ == "__main__":
    main()
