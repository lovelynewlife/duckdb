import duckdb
import joblib
import numpy as np
import pandas as pd
from duckdb.typing import BIGINT, DOUBLE, FLOAT, VARCHAR
from sklearn import feature_extraction, naive_bayes
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfTransformer
import time
from tqdm import tqdm

# hand_type = "udf"
hand_type = "special"
name = "uc04"
scale = 40

con = duckdb.connect(
    f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_tpcx_ai_sf{scale}.db")

root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"

model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"


model = joblib.load(model_file_name)

def udf(txt):
    data = pd.DataFrame({
        "text": txt
    })
    # print(data.shape)
    return model.predict(data["text"])

sql='''
select txt from 
(select DISTINCT text txt from Review);
'''
times=5
min1=0
max1=0
res=0
flag=True
for i in tqdm(range(times)):
    s=time.perf_counter()
    res_data=con.sql(sql).fetch_arrow_table()
    udf(*res_data)
    e=time.perf_counter()
    t=e-s
    print(f"{i+1} : {t}")
    res=res + t
    if flag:
        flag=False
        min1=t
        max1=t
    else:
        min1=t if min1 > t else min1
        max1=t if max1 < t else max1
print(f"min : {min1}")
print(f"max : {max1}")
res=res - min1 - max1
times=times - 2
print(f"{name}, {res/times}s ")
