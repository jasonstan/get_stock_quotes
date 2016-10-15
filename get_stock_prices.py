"""
Program to get regular updates on stock price for largest companies in 
the world by market cap, and to store this data in a postgres database.

Author: Jason Stanley
Last modified: 15 Oct 2016

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
import requests
import json
from bs4 import BeautifulSoup
from googlefinance import getQuotes


def get_company_data(s):
    """
    For given stock, return dictionary containing company name and index 
    on which stock is traded.
    """

    # specify user agent to facilitate scraping
    head = {"User-Agent":("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/51.0.2704.103 "
                          "Safari/537.36")}
    
    # scrape and parse
    html = requests.get('https://www.google.com/finance?q={0}'.format(s), 
                        headers=head).content
    soup = BeautifulSoup(html, "html.parser")
    
    # create company dict and fill with relevant data
    company_dict = {}
    company_dict['stock_symbol'] = s
    company_dict['name'] = soup.find("div", 
        class_="appbar-snippet-primary").text
    company_dict['index'] = soup.find("div", 
        class_="appbar-snippet-secondary").text.split(":")[0][1:]

    return company_dict


def insert_company_data(company_dict):
    """Insert general company data into database."""

    conn = pg.connect(database='stocks')
    cur = conn.cursor()

    q = """
        INSERT INTO company (stock_symbol, company_name, index) 
        VALUES (%s,%s, %s)
        """

    cur.execute(q, [
        company_dict['stock_symbol'],
        company_dict['name'],
        company_dict['index']
        ]
    )  
    conn.commit()
    conn.close()


def get_companies_data(stocks):
    """For each stock in list, get company data and insert into db."""

    for stock in stocks:
        print("retrieving company data for", stock)
        insert_company_data(get_company_data(stock))


def get_stock_price(s):
    """
    For given stock, return dictionary containing stock symbol, last 
    traded price, last trade datetime, and index on which stock is traded.
    """

    stock_dict = {}
    stock_dict["stock_symbol"] = s
    stock_dict["last_traded_price"] = getQuotes(s)[0]['LastTradePrice']
    stock_dict["last_trade_datetime"] = getQuotes(s)[0]['LastTradeDateTime']
    stock_dict["index"] = getQuotes(s)[0]['Index']

    return stock_dict


def insert_stock_price(stock_dict):
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

    cur.execute(q, [
        stock_dict["stock_symbol"],
        stock_dict["last_traded_price"],
        stock_dict["last_trade_datetime"],
        stock_dict["index"],
        stock_dict["stock_symbol"],
        stock_dict["last_trade_datetime"]
        ]
    )  
    conn.commit()
    conn.close()


def get_stock_data(stocks, periods, delay):
    """
    For N periods separated by delay of duration X, get stock quotes and 
    insert in db.
    """

    i = 1
    while i <= periods:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), 
              "commencing quote loop:", i)
        for stock in stocks:
            print("retrieving quote for", stock)
            insert_stock_price(get_stock_price(stock))
        i += 1
        time.sleep(delay)
    

def build_stocks_db(stocks, periods, delay):
    """Execute major functions to fill database."""

    get_companies_data(stocks)
    get_stock_data(stocks, periods, delay)
    


# key input variables
STOCKS = ['NASDAQ:AAPL', 'NASDAQ:GOOGL', 'NASDAQ:MSFT', 'NASDAQ:AMZN', 
          'NASDAQ:FB', 'NYSE:XOM', 'NYSE:BRK.A', 'NYSE:JNJ', 'NYSE:BABA', 
          'NYSE:GE', 'NYSE:CHL']
NUM_QUOTES = 1              # number of loops to perform
DELAY_BETWEEN_QUOTES = 0    # delay between stock quote updates in seconds


# execute
build_stocks_db(STOCKS, NUM_QUOTES, DELAY_BETWEEN_QUOTES)
