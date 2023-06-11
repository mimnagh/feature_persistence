from datetime import datetime
import pandas as pd
import pytest

from registry import register_data_frame, reset_registry
from persistence import TimeSeriesDataPersisterFactory, PersistenceManager
from settings import get_settings

@pytest.fixture()
def pm(tmp_path):
    settings = get_settings()
    settings.storage_path = tmp_path

    reset_registry()
    return PersistenceManager(settings)

def test_persist_data(pm):

    test_data3 = pd.DataFrame({"date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
                              "col1": [1.0, 2.0, 3.0],
                              "col2": ["1.0", "2.0", "3.0"]})
    register_data_frame("test3", test_data3, {"test": "test"})
    pm.write_data_frame("test3", datetime.now(), test_data3)

def test_read_data(pm):
    reset_registry()
    test_data3 = pd.DataFrame({"date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
                              "col1": [1.0, 2.0, 3.0],
                              "col2": ["1.0", "2.0", "3.0"]})
    register_data_frame("test3", test_data3, {"test": "test"})

    pm = PersistenceManager()
    as_of_dt = pm.write_data_frame("test3", datetime.now(), test_data3)
    dt_actual = pm.read_data_frame("test3", as_of_dt)

    assert dt_actual.equals(test_data3)

def test_delete_data(pm):
    reset_registry()
    test_data3 = pd.DataFrame({"date": [datetime(2021, 1, 1), datetime(2021, 1, 2), datetime(2021, 1, 3)],
                              "col1": [1.0, 2.0, 3.0],
                              "col2": ["1.0", "2.0", "3.0"]})
    register_data_frame("test3", test_data3, {"test": "test"})

    pm = PersistenceManager()
    as_of_dt = pm.write_data_frame("test3", datetime.now(), test_data3)
    pm.delete_time_series("test3.col1", as_of_dt)
    pm.delete_time_series("test3.col2", as_of_dt)

    with pytest.raises(Exception) as e_info:
        pm.read_data_frame("test3", as_of_dt)

