import yfinance as yf


def extract_yahoo(symbol, start_date, end_date):
    """
    :param symbol: str symbol to download
    :param start_date: str YYYY-MM-DD
    :param end_date: str YYYY-MM-DD
    :return:
    """
    return yf.download(symbol, start=start_date, end=end_date)
