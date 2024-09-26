import joblib
import numpy as np
import pyarrow as pa
import pandas as pd
import joblib
from surprise import SVD
from surprise import Dataset
from surprise.reader import Reader



def process_table(table):
    scale = 10
    name = "uc10"
    root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"
    model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
    model = joblib.load(model_file_name)

    def udf(business_hour_norm, amount_norm):
        data = pd.DataFrame({
            'business_hour_norm': business_hour_norm,
            'amount_norm': amount_norm
        })
        # print(data.shape)
        return model.predict(data)
    df = pd.DataFrame(udf(*table))
    print(len(df))
    return pa.Table.from_pandas(df)

class MyProcess:
    def __init__(self):
        # load model part
        pass

    def process(self, table):
        # print(table.num_rows)
        return process_table(table)
