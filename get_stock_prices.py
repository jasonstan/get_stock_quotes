"""
Program to get regular updates on stock price for largest companies in 
the world by market cap, and to store this data in a postgres database.

Author: Jason Stanley
Last modified: 11 Oct 2016

----

googlefinance module's getQuote function procudes the following output 
for sample stock ('AAPL'):

[
  {
    "LastTradeWithCurrency": "114.06",
    "ID": "22144",
    "LastTradeDateTimeLong": "Oct 7, 4:00PM EDT",
    "LastTradeTime": "4:00PM EDT",
    "Index": "NASDAQ",
    "StockSymbol": "AAPL",
    "LastTradePrice": "114.06",
    "LastTradeDateTime": "2016-10-07T16:00:02Z"
  }
]
"""


# import necessary libraries
import pandas as pd
import psycopg2 as pg
import time


def get_company_data(s):
    """
    For given stock symbol, return two str variables: 1) company name, 
    and 2) index on which stock is traded.
    """

    from bs4 import BeautifulSoup
    import requests

    head = {"User-Agent":("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/51.0.2704.103 "
                          "Safari/537.36")}
    html = requests.get('https://www.google.com/finance?q={0}'.format(s), 
                        headers=head).content
    soup = BeautifulSoup(html, "html.parser")
    name = soup.find("div", class_="appbar-snippet-primary").text
    index = soup.find("div", class_="appbar-snippet-secondary").text
    index = index.split(":")[0][1:]
    return name, index


def insert_company_data(s, name, index):
    """
    For given stock symbol, create record in company table of db 
    containing stock symble, company name, and index on which stock is 
    traded.
    """

    conn = pg.connect(database='stocks')
    cur = conn.cursor()

    q = """
        INSERT INTO company (stock_symbol, company_name, index) 
        VALUES (%s,%s, %s)
        """

    cur.execute(q, [s, name, index])  
    conn.commit()
    conn.close()


def get_companies_data(stocks):
    """For each stock in list, get company data and insert into db."""

    for stock in stocks:
        name, index = get_company_data(stock)
        insert_company_data(stock, name, index)


def get_stock_price(s):
    """
    For given stock symbol, return four variables: stock symbol, 
    last traded price, last trade datetime, and index on which stock is 
    traded.
    """

    from googlefinance import getQuotes
    import json

    last_traded_price = getQuotes(s)[0]['LastTradePrice']
    last_trade_datetime = getQuotes(s)[0]['LastTradeDateTime']
    index = getQuotes(s)[0]['Index']

    return s, last_traded_price, last_trade_datetime, index


def insert_stock_price(s, last_traded_price, last_trade_datetime, index):
    """Insert stock data into database."""

    conn = pg.connect(database='stocks')
    cur = conn.cursor()

    q = """
        INSERT INTO stock_prices (
            stock_symbol, 
            last_traded_price, 
            last_trade_datetime, 
            index
        ) SELECT %s,%s,%s,%s WHERE NOT EXISTS (
                      SELECT *
                        FROM stock_prices
                       WHERE stock_symbol = (%s)
                         AND last_trade_datetime = (%s));
    """

    cur.execute(q, [s, last_traded_price, last_trade_datetime, index,
                    s, last_trade_datetime])  
    conn.commit()
    conn.close()


def get_stock_data(stocks, periods, delay):
    """
    For N periods separated by delay of duration X, get stock quotes and 
    insert in db.
    """

    i = 1
    while i <= periods:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), "loop:", i)
        for stock in stocks:
            print("processing", stock)
            s, last_traded_price, last_trade_datetime, index = get_stock_price(stock)
            insert_stock_price(s, last_traded_price, last_trade_datetime, index)
        i += 1
        time.sleep(delay)
    

def build_stocks_db(stocks, periods, delay):
    """Execute major functions to fill database."""

    get_companies_data(stocks)
    get_stock_data(stocks, periods, delay)
    


# key input variables
stocks = ['NASDAQ:AAPL', 'NASDAQ:GOOGL', 'NASDAQ:MSFT', 'NASDAQ:AMZN', 
          'NASDAQ:FB', 'NYSE:XOM', 'NYSE:BRK.A', 'NYSE:JNJ', 'NYSE:BABA', 
          'NYSE:GE', 'NYSE:CHL']    # stocks of interest
periods = 1       # number of loops to perform
delay = 60*60     # delay between stock quote updates = one hour


# execute
build_stocks_db(stocks, periods, delay)
