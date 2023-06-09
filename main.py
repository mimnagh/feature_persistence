import os
from datetime import datetime

import pandas as pd

from persistence import TimeSeriesDataPersisterFactory, PersistenceManager
from registry import get_registry, register_float_time_series, register_str_time_series, register_data_frame, \
    unregister_namespace
from settings import get_settings

# def read_time_series(registration_key: str) -> pd.DataFrame:
#     if registration_key in get_registry():
#         return pd.read_parquet(get_registry()[registration_key]["file_path"])
#     else:
#         raise Exception("Time series not registered with key: " + registration_key)
#
# def write_time_series(namespace: str,  time_series: pd.DataFrame):
#     values_column_name = time_series.columns[1]
#     registration_key = namespace + "." + values_column_name
#     if registration_key in get_registry():
#         file_path = get_registry()[registration_key]["file_path"]
#         dir_part = os.path.dirname(file_path)
#         os.makedirs(dir_part, exist_ok=True)
#         time_series.to_parquet(get_registry()[registration_key]["file_path"])
#     else:
#         raise Exception("Time series not registered with key: " + registration_key)
#
# def write_data_frame(namespace: str,  data_frame: pd.DataFrame):
#     for column_name in data_frame.columns[1:]:
#         write_time_series(namespace, data_frame[[data_frame.columns[0], column_name]])
#
# def read_data_frame(namespace: str) -> pd.DataFrame:
#     data_frame = pd.DataFrame()
#     for key in get_registry().keys():
#         value = get_registry()[key]
#         if value["namespace"] == namespace:
#             if len(data_frame) == 0:
#                 data_frame = read_time_series(key)
#             else:
#                 time_series = read_time_series(key)
#                 data_frame[time_series.columns[1]] = time_series[time_series.columns[1]]
#     return data_frame

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

    print(get_registry())

    pm = PersistenceManager()

    as_of = pm.write_data_frame("test3",
                        datetime.now(),
                        test_data3)
    print(
        read_data_frame("test3", as_of)
    )

    unregister_namespace("test3")

    print(get_registry())


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
