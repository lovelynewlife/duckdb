import pickle

import duckdb
import numpy as np
from duckdb.typing import BIGINT, DOUBLE, FLOAT, VARCHAR
import pandas as pd
import time
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from catboost import CatBoostClassifier
import time
from tqdm import tqdm
# hand_type = "udf"
hand_type = "special"
name = "pf8"


con = duckdb.connect(
    "/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_raven_1G.db")

root_model_path = "/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/test_raven"


scaler_path = f'{root_model_path}/Credit_Card/creditcard_standard_scale_model.pkl'
model_path = f'{root_model_path}/Credit_Card/creditcard_catboost_gb.cbm'
with open(scaler_path, 'rb') as f:
    scaler = pickle.load(f)
model = CatBoostClassifier()
model.load_model(model_path)



def udf(V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount):
    data = np.column_stack(
        [V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount])
    numerical = np.column_stack(data.T)
    
    X = scaler.transform(numerical)
    return model.predict(X)


sql = '''
SELECT V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15,
 V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount FROM Credit_Card_extension 
 WHERE V1 > 1 AND V2 < 0.27 AND V3 > 0.3;
'''
times = 5
min1 = 0
max1 = 0
res = 0
flag = True
for i in tqdm(range(times)):
    s = time.perf_counter()
    res_data = con.sql(sql).fetch_arrow_table()
    udf(*res_data)
    e = time.perf_counter()
    t = e-s
    print(f"{i+1} : {t}")
    res = res + t
    if flag:
        flag = False
        min1 = t
        max1 = t
    else:
        min1 = t if min1 > t else min1
        max1 = t if max1 < t else max1
print(f"min : {min1}")
print(f"max : {max1}")
res = res - min1 - max1
times = times - 2
print(f"{name}, {res/times}s ")
