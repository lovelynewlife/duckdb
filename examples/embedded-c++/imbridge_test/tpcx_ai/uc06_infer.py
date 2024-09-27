import duckdb
import joblib
import numpy as np
import pandas as pd
from duckdb.typing import BIGINT, DOUBLE, FLOAT, VARCHAR
from imblearn.over_sampling import ADASYN
from imblearn.pipeline import Pipeline as Imb_Pipeline
import joblib
from sklearn import metrics
from sklearn.metrics import classification_report, confusion_matrix, f1_score
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from tqdm import tqdm
import time
from tqdm import tqdm

# hand_type = "udf"
hand_type = "special"
name = "uc06"
scale = 10

con = duckdb.connect(
    f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_tpcx_ai_sf{scale}.db")

root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"

model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"


model = joblib.load(model_file_name)

def udf(smart_5_raw,
        smart_10_raw,
        smart_184_raw,
        smart_187_raw,
        smart_188_raw,
        smart_197_raw,
        smart_198_raw):
    data = pd.DataFrame({
        'smart_5_raw': smart_5_raw,
        'smart_10_raw': smart_10_raw,
        'smart_184_raw': smart_184_raw,
        'smart_187_raw': smart_187_raw,
        'smart_188_raw': smart_188_raw,
        'smart_197_raw': smart_197_raw,
        'smart_198_raw': smart_198_raw
    })
    # print(data.shape)
    return model.predict(data)


sql='''
select smart_5_raw, smart_10_raw, smart_184_raw, smart_187_raw, smart_188_raw, smart_197_raw, smart_198_raw
from Failures;
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
