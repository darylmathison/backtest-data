import fill_data as fd
import models as model
import csv
from sqlalchemy import select, func, and_, table


def get_best_risk_reward():
    # Subquery for the inner join
    subquery = (
        select(
            model.RiskReward.symbol,
            func.max(model.RiskReward.portion_to_risk).label("max_portion_to_risk"),
        )
        .select_from(model.RiskReward)
        .where(
            and_(
                model.RiskReward.portion_to_risk > 0,
                model.RiskReward.symbol.in_(
                    select(model.Assets.symbol).where(
                        and_(
                            model.Assets.dividend,
                            model.Assets.percentage_downloaded > 0.8,
                            model.Assets.percentage_downloaded < 1.2,
                            model.Assets.beta > 0.95,
                            model.Assets.beta < 3,
                        )
                    )
                ),
            )
        )
        .group_by(model.RiskReward.symbol)
        .order_by(model.RiskReward.symbol)
        .subquery(name="a")
    )

    # Main query
    query = (
        select(
            model.RiskReward.symbol,
            model.RiskReward.portion_to_risk,
            model.RiskReward.div_multiplier,
            model.RiskReward.stop_loss_percentage,
        )
        .select_from(
            model.RiskReward.__table__.join(
                subquery,
                and_(
                    model.RiskReward.symbol == subquery.c.symbol,
                    model.RiskReward.portion_to_risk == subquery.c.max_portion_to_risk,
                ),
            )
        )
        .order_by(
            model.RiskReward.portion_to_risk.desc(), model.RiskReward.avg_gain.desc()
        )
    )

    with fd.open_session() as dbsession:
        return dbsession.execute(query).all()


def main():
    symbol_to_filter = set()

    def result_filter(row):
        if row[0] not in symbol_to_filter:
            symbol_to_filter.add(row[0])
            return True
        return False

    results = get_best_risk_reward()
    reduced_results = [row for row in results if result_filter(row)]
    print(len(reduced_results))
    with open("best_risk_reward.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "priority",
                "symbol",
                "portion_to_risk",
                "div_multiplier",
                "stop_loss_percentage",
            ]
        )
        for i, row in enumerate(reduced_results):
            writer.writerow((i, row[0], round(row[1], 3), row[2], row[3]))


if __name__ == "__main__":
    main()
