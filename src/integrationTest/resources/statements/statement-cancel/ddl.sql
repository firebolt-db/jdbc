DROP TABLE IF EXISTS "ex_lineitem" CASCADE;
DROP TABLE IF EXISTS "first_statement_cancel_test" CASCADE;
DROP TABLE IF EXISTS "second_statement_cancel_test" CASCADE;
CREATE FACT TABLE IF NOT EXISTS ex_lineitem ( l_orderkey LONG );
CREATE FACT TABLE IF NOT EXISTS first_statement_cancel_test ( l_orderkey LONG ) PRIMARY INDEX l_orderkey;
CREATE FACT TABLE IF NOT EXISTS second_statement_cancel_test ( l_orderkey LONG ) PRIMARY INDEX l_orderkey;