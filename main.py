import os
from datetime import datetime

import pandas as pd
import numpy as np
import pandera as pa

# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
registry = dict()
float_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(datetime),
        "values_float": pa.Column(float),
    })
str_schema = pa.DataFrameSchema(
    {
        "date": pa.Column(datetime),
        "values_str": pa.Column(str),
    })

def get_registery():
    return registry

# Register a target time series represented as a data frame with the first column containing date.times
# and the second column containing values. Concatenates a supplied namespace and the values column name.
def register_float_time_series(namespace: str, time_series: pd.DataFrame, registration_data: dict):
    time_series = time_series.copy()
    values_column_name = time_series.columns[1]
    registration_key = namespace + "." + values_column_name
    if( registration_key in get_registery()):
        raise Exception("Time series already registered with key: " + registration_key)
    time_series.columns = ["date", "values_float"]
    float_schema.validate(time_series)
    registration_data.update({"type": "float",
                                  "namespace": namespace,
                                  "values_column_name": values_column_name,
                              "file_path": "data/" + namespace + "/" + values_column_name + ".parquet"})
    registry[registration_key] = registration_data
    print("Registering time series with key: " + registration_key)

def register_str_time_series(namespace: str, time_series: pd.DataFrame, user_registration_data: dict):
    time_series = time_series.copy()
    values_column_name = time_series.columns[1]
    registration_key = namespace + "." + values_column_name
    if( registration_key in get_registery()):
        raise Exception("Time series already registered with key: " + registration_key)
    time_series.columns = ["date", "values_str"]
    str_schema.validate(time_series)
    default_registration_data = {"type": "str",
                                  "namespace": namespace,
                                  "values_column_name": values_column_name,
                                "file_path": "data/" + namespace + "/" + values_column_name + ".parquet"}
    default_registration_data.update(user_registration_data)
    registry[registration_key] = default_registration_data
    print("Registering time series with key: " + registration_key)

def register_time_series(namespace: str, time_series: pd.DataFrame, user_registration_data: dict):
    if(isinstance(time_series.iloc[0,1], str)):
        register_str_time_series(namespace, time_series, user_registration_data)
    else:
        register_float_time_series(namespace, time_series, user_registration_data)

def register_data_frame(namespace: str, data_frame: pd.DataFrame, user_registration_data: dict):
    for column_name in data_frame.columns[1:]:
        register_time_series(namespace, data_frame[[data_frame.columns[0], column_name]], user_registration_data)

def unregister_namespace(namespace: str):
    for key in list(registry.keys()):
        value = registry[key]
        if value["namespace"] == namespace:
            del registry[key]
            print("Unregistering time series with key: " + key)
def unregister_key(key: str):
    if key in registry:
        del registry[key]
        print("Unregistering time series with key: " + key)

def unregister_time_series(namespace: str, time_series: pd.DataFrame):
    values_column_name = time_series.columns[1]
    registration_key = namespace + "." + values_column_name
    unregister_key(registration_key)

def read_time_series(registration_key: str) -> pd.DataFrame:
    if registration_key in registry:
        return pd.read_parquet(registry[registration_key]["file_path"])
    else:
        raise Exception("Time series not registered with key: " + registration_key)

def write_time_series(namespace: str,  time_series: pd.DataFrame):
    values_column_name = time_series.columns[1]
    registration_key = namespace + "." + values_column_name
    if registration_key in registry:
        file_path = registry[registration_key]["file_path"]
        dir_part = os.path.dirname(file_path)os.makedirs(path, exist_ok=True)
        os.makedirs(dir_part, exist_ok=True)
        time_series.to_parquet(registry[registration_key]["file_path"])
    else:
        raise Exception("Time series not registered with key: " + registration_key)

def write_data_frame(namespace: str,  data_frame: pd.DataFrame):
    for column_name in data_frame.columns[1:]:
        write_time_series(namespace, data_frame[[data_frame.columns[0], column_name]])

def read_data_frame(namespace: str) -> pd.DataFrame:
    data_frame = pd.DataFrame()
    for key in registry.keys():
        value = registry[key]
        if value["namespace"] == namespace:
            if len(data_frame) == 0:
                data_frame = read_time_series(key)
            else:
                time_series = read_time_series(key)
                data_frame[time_series.columns[1]] = time_series[time_series.columns[1]]
    return data_frame

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    test_data1 = pd.DataFrame({"date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
                                "values_float": [1.0, 2.0, 3.0]})
    register_float_time_series("test1", test_data1, {"test": "test"})

    test_data2 = pd.DataFrame({"date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
                                "values_str": ["a", "b", "c"]})
    register_str_time_series("test2", test_data2, {"test": "test"})

    test_data3 = pd.DataFrame({"date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
                              "col1": [1.0, 2.0, 3.0],
                              "col2": ["1.0", "2.0", "3.0"]})
    register_data_frame("test3", test_data3, {"test": "test"})

    print(registry)

    write_time_series("test1", test_data1)

    read_time_series("test1.values_float")

    write_time_series("test2", test_data2)

    read_time_series("test2.values_str")

    write_data_frame("test3", test_data3)

    read_time_series("test3.col1")

    print(
        read_data_frame("test3")
    )

    unregister_namespace("test3")

    print(registry)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
