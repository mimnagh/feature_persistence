import os
from datetime import datetime

import pandas as pd
import pandera as pa

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

registry = dict()
def get_registry():
    return registry

# Register a target time series represented as a data frame with the first column containing date.times
# and the second column containing values. Concatenates a supplied namespace and the values column name.
def register_float_time_series(namespace: str, time_series: pd.DataFrame, registration_data: dict):
    time_series = time_series.copy()
    values_column_name = time_series.columns[1]
    registration_key = namespace + "." + values_column_name
    if( registration_key in get_registry()):
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
    if( registration_key in get_registry()):
        raise Exception("Time series already registered with key: " + registration_key)
    time_series.columns = ["date", "values_str"]
    str_schema.validate(time_series)
    default_registration_data = {"type": "str",
                                  "namespace": namespace,
                                  "values_column_name": values_column_name}
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
