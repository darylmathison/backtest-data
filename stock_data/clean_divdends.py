from sqlalchemy import and_
from sqlalchemy.orm import Session, aliased

from stock_data.fill_data import open_session
from stock_data.models import Dividends

from collections import defaultdict


def gather_dups(session: Session):
    pairs = defaultdict(list)
    found_right_ids = set()
    d1 = aliased(Dividends)
    d2 = aliased(Dividends)
    query = (
        session.query(d1.id, d2.id)
        .join(
            d2,
            and_(
                d1.symbol == d2.symbol,
                d1.ex_dividend_date == d2.ex_dividend_date,
                d1.id != d2.id,
            ),
        )
        .order_by(d1.id)
    )
    for left, right in query.all():
        if left not in found_right_ids:
            pairs[left].append(right)
            found_right_ids.add(right)
    ids_to_delete = set()
    for key, value in pairs.items():
        if value:
            ids_to_delete.update(value)

    return ids_to_delete


if __name__ == "__main__":
    with open_session() as session:
        id_to_delete = gather_dups(session)
        for dup in session.query(Dividends).filter(Dividends.id.in_(id_to_delete)):
            session.delete(dup)
        session.commit()
