/* create table script for stock price information */
CREATE TABLE public.staging_stock_price (
  ticker text,
  "open" decimal(18,9),
  close decimal(18,9),
  adj_close decimal(18,9),
  low decimal(18,9),
  high decimal(18,9),
  volume bigint,
  "date" date
);

/* create table script for stock information */
CREATE TABLE public.staging_stock_info (
  ticker text,
  exchange text,
  name text,
  sector text,
  industry text
);
