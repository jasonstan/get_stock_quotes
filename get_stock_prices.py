"""
Program to get regular updates on stock price for largest companies in the
world by market cap, and to store this data in a postgres database.

Author: Jason Stanley
Last modified: 09 Oct 2016

----

googlefinance module's getQuote function procudes the following output for 
sample stock ('AAPL'):

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
import psycopg2 as pg
import time


def create_company(cur):
    '''
    Create company table to store slowly changing fields for each company
    '''

    q = """
        CREATE TABLE IF NOT EXISTS company (\
            id serial not null, \
            created_at timestamp not null default now(), \
            stock_symbol varchar(10) default null, \
            company_name varchar(20) default null, \
            index varchar (20) default null);
        """

    cur.execute(q)


def create_stock_prices(cur):
    '''
    Create stock_prices table to store regular updates on stock price
    '''
    
    q = """
        CREATE TABLE IF NOT EXISTS stock_prices (\
            id serial not null,
            created_at timestamp not null default now(), \
            stock_symbol varchar(10) default null, \
            last_traded_price money default null, \
            last_trade_datetime timestamp default null, \
            index varchar (20) default null);
        """

    cur.execute(q)


def create_table_structure():
    '''
    Assemble table structure of database
    '''

    conn = pg.connect(database='stocks')
    cur = conn.cursor()
    
    create_stock_prices(cur)    # stock_prices table
    create_company(cur)         # company table
    
    conn.commit()
    conn.close()


def insert_company_data(s):
    '''
    Given stock symbol, create record in db for high-level company info
    '''

    conn = pg.connect(database='stocks')
    cur = conn.cursor()

    q = """
    INSERT INTO company (stock_symbol, company_name) VALUES (%s,%s)
    """

    cur.execute(q, [s, s+s])  
    conn.commit()
    conn.close()


def get_stock_price(s):
    '''
    For given stock symbol, get last traded price, last trade datetime,
    index on which stock is traded
    '''

    from googlefinance import getQuotes
    import json

    last_traded_price = getQuotes(s)[0]['LastTradePrice']
    last_trade_datetime = getQuotes(s)[0]['LastTradeDateTime']
    index = getQuotes(s)[0]['Index']

    return s, last_traded_price, last_trade_datetime, index


def insert_stock_price(s, last_traded_price, last_trade_datetime, index):
    '''
    Given stock symbol and data on last traded price and date,
    insert into database
    '''

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
    '''
    For (periods) loops and (stocks) list of stocks, collects and stores key 
    stock price information at (delay) inrterval
    '''

    # create table structure if not exists
    create_table_structure()
    insert_company_data('AAPL')

    i = 1
    while i <= periods:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), "loop:", i)
        for stock in stocks:
            print("processing", stock)
            s, last_traded_price, last_trade_datetime, index = get_stock_price(stock)
            insert_stock_price(s, last_traded_price, last_trade_datetime, index)
        i += 1
        time.sleep(delay)


# key input variables
stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'FB', 'XOM', 'BRK-A', 'JNJ', 'BABA', 
          'NYSE:GE', 'CHL']    # stocks of interest
periods = 2     # number of loops to perform
delay = 5.0     # seconds


# execute
get_stock_data(stocks, periods, delay)
