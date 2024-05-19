_calender = None


def create_calendar():
    from business_calendar import Calendar, MO, TU, WE, TH, FR
    from stock_data.fill_data import open_session
    from stock_data.models import Holidays

    global _calender
    if _calender is None:
        with open_session() as session:
            holidays = session.query(Holidays).all()
            holidays = [holiday.date for holiday in holidays]
            _calender = Calendar(
                workdays=[MO, TU, WE, TH, FR],
                holidays=holidays,
            )

    return _calender


def convert_to_currency(value: float) -> float:
    return round(value + 0.0005, 2)
