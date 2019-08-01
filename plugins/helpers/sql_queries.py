# select statement to pull all fields from the staging_stock_price table
class SqlQueries:
    fact_stock_price_table_insert = ("""
        SELECT  ticker, date, 'open', close, adj_close, high, low, volume FROM staging_stock_price 
    """)
