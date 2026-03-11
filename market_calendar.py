from datetime import datetime
from nse_holidays import NSE_HOLIDAYS


def is_market_day():

    today = datetime.now().date()

    # Weekend
    if today.weekday() >= 5:
        return False

    # NSE holiday
    if today in NSE_HOLIDAYS:
        return False

    return True