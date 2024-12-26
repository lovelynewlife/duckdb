import duckdb.functional
import pytest
import duckdb

import numpy as np
from duckdb.typing import BIGINT, DOUBLE, FLOAT, VARCHAR, BOOLEAN
import pandas as pd


def udf(a, b):
    print(len(a))
    return a

con = duckdb.connect(":memory:")
con.create_function("udf", udf,
            [DOUBLE, DOUBLE], DOUBLE, type="arrow", kind=duckdb.functional.PREDICTION, batch_size=4444
            )

print(duckdb.functional.PREDICTION)
print(duckdb.functional.COMMON)

con.sql("create table t1(a double, b double);")
for i in range(4050):
    con.sql("insert into t1 values (2.0, 3.0), (4.0, 7.0);")

con.sql("create table t2(a double, b double);")
con.sql("insert into t2 values (2.0, 3.0), (4.0, 7.0);")
# con.sql("SET threads = 1;")
con.table("t1").show()

def project_test():
    res = con.sql('''
    explain SELECT udf(a,b), a, b FROM t1;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT avg(c) FROM (SELECT udf(a,b) as c, a, b FROM t1) GROUP BY a;
    ''').explain()
    print(res)

def filter_test():
    res = con.sql('''
    explain SELECT a, b FROM t1 where udf(a,b) > 2.2 and cos(a) > -1;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT a, b FROM t1 where udf(a,b) > 2.2 and cos(a) > -1;
    ''').show()

    res = con.sql('''
    explain SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2;
    ''').fetchall()

    for elem in res[0]:
         print(elem)
    
    res = con.sql('''
    SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2;
    ''').show()

    res = con.sql('''
    explain SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2 and t1.a+t2.b > 5.2;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2 and t1.a+t2.b > 5.2;
    ''').show()

def tpc_h_test():
    con.sql("CALL dbgen(sf = 1);")
    
    print(con.sql('''
        SELECT c_custkey,
               c_name,
               sum(l_extendedprice * (1 - l_discount)) as revenue,
               c_acctbal,
               n_name,
               c_address,
               c_phone,
               c_comment
       from customer, orders, lineitem, nation
       where c_custkey = o_custkey and
             l_orderkey = o_orderkey and
             o_orderdate >= DATE'1993-10-01' and
             o_orderdate < DATE'1993-10-01' + interval '3' month and
             c_nationkey = n_nationkey and udf(l_orderkey, o_custkey) > 0 
       group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment 
       order by revenue desc
''').explain())
    
    con.sql('''
        SELECT c_custkey,
               c_name,
               sum(l_extendedprice * (1 - l_discount)) as revenue,
               c_acctbal,
               n_name,
               c_address,
               c_phone,
               c_comment
       from customer, orders, lineitem, nation
       where c_custkey = o_custkey and
             l_orderkey = o_orderkey and
             o_orderdate >= DATE'1993-10-01' and
             o_orderdate < DATE'1993-10-01' + interval '3' month and
             c_nationkey = n_nationkey and udf(l_orderkey, o_custkey) > 0 
       group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment 
       order by revenue desc
''').show()


if __name__ == "__main__":
    # project_test()
    # filter_test()
    tpc_h_test()