DROP TABLE IF EXISTS "first_statement_cancel_test" CASCADE;
DROP TABLE IF EXISTS "second_statement_cancel_test" CASCADE;
CREATE
EXTERNAL TABLE IF NOT EXISTS ex_lineitem ( l_orderkey LONG, l_partkey LONG, l_suppkey LONG, l_linenumber INT, l_quantity LONG, l_extendedprice LONG, l_discount LONG, l_tax LONG, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT)URL = 's3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/'OBJECT_PATTERN = '*.parquet'TYPE = (PARQUET);
CREATE
FACT TABLE IF NOT EXISTS first_statement_cancel_test ( l_orderkey LONG, l_partkey LONG, l_suppkey LONG, l_linenumber INT, l_quantity LONG, l_extendedprice LONG, l_discount LONG, l_tax LONG, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT ) PRIMARY INDEX l_orderkey, l_linenumber;
CREATE
FACT TABLE IF NOT EXISTS second_statement_cancel_test ( l_orderkey LONG, l_partkey LONG, l_suppkey LONG, l_linenumber INT, l_quantity LONG, l_extendedprice LONG, l_discount LONG, l_tax LONG, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT ) PRIMARY INDEX l_orderkey, l_linenumber;