-- Creates table structure for database 'stocks'

DROP TABLE IF EXISTS company, stock_prices;

-- Create company table to store slowly changing fields for each company
CREATE TABLE company(
    id serial not null primary key,
    created_at timestamp not null default now(),
    stock_symbol varchar(10) default null,
    company_name varchar(50) default null,
    index varchar (20) default null
);


-- Create stock_prices table to store regular updates on stock price
CREATE TABLE stock_prices(
    id serial not null primary key,
    created_at timestamp not null default now(),
    stock_symbol varchar(10) default null,
    last_traded_price money default null,
    last_trade_datetime timestamp default null,
    index varchar (20) default null
);
