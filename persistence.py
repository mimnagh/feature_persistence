import datetime
import os
from abc import ABC, abstractmethod
import pandas as pd

import registry
from settings import get_settings, AppSettings


class PersistenceManager:
    def __init__(self, settings: AppSettings = get_settings()):
        self.persister = TimeSeriesDataPersisterFactory.create_persister(settings)

    def write_time_series(self, key: str, as_of: datetime.datetime, time_series: pd.DataFrame):
        print("Writing time series with key: " + key)
        if key in registry.get_registry():
            self.persister.persist_data(key, as_of, time_series)
        else:
            raise Exception("Time series not registered with key: " + key)

    def read_time_series(self, key: str, as_of: datetime.datetime)-> pd.DataFrame:
        if key in registry.get_registry():
            return self.persister.read_data(key, as_of)
        else:
            raise Exception("Time series not registered with key: " + key)
    def delete_time_series(self, key: str, as_of: datetime.datetime):
        self.persister.delete_data(key, as_of)

    def write_data_frame(self, namespace: str,  as_of: datetime.datetime,  data_frame: pd.DataFrame) -> datetime.datetime:
        for column_name in data_frame.columns[1:]:
            registration_key = namespace + "." + column_name
            self.write_time_series(registration_key, as_of, data_frame[[data_frame.columns[0], column_name]])
        return as_of

    def read_data_frame(self, namespace: str,  as_of: datetime.datetime) -> pd.DataFrame:
        data_frame = pd.DataFrame()
        for key in registry.get_registry().keys():
            value = registry.get_registry()[key]
            # TODO need a bi-directional mapping between namespace and registration key
            if value["namespace"] == namespace:
                if len(data_frame) == 0:
                    data_frame = self.read_time_series(key, as_of)
                else:
                    time_series = self.read_time_series(key, as_of)
                    data_frame[time_series.columns[1]] = time_series[time_series.columns[1]]
        return data_frame

class TimeSeriesDataPersister(ABC):
    @abstractmethod
    def persist_data(self, key, as_of, data_frame):
        pass

    @abstractmethod
    def read_data(self, key, as_of):
        pass

    @abstractmethod
    def delete_data(self, key, as_of):
        pass


class TimeSeriesDataPersisterFactory:
    @staticmethod
    def create_persister(settings):
        persister_type = settings.persistence_type
        if persister_type == 'local_file':
            return LocalFilePersister(settings)
        elif persister_type == 's3':
            return S3Persister(settings)
        elif persister_type == 'sql_db':
            return SQLDBPersister(settings)
        else:
            raise ValueError(f"Unknown persister type: {persister_type}")

class LocalFilePersister(TimeSeriesDataPersister):
    def __init__(self, settings):
        self.storage_path = settings.storage_path

    def persist_data(self, key, as_of, data_frame):
        file_path = self._get_file_path(key, as_of)
        print("Writing time series to file: " + file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        data_frame.to_parquet(file_path, index=False)

    def read_data(self, key, as_of):
        file_path = self._get_file_path(key, as_of)
        return pd.read_parquet(file_path)

    def delete_data(self, key, as_of):
        file_path = self._get_file_path(key, as_of)
        import os
        os.remove(file_path)

    def _get_file_path(self, key, as_of):
        if key in registry.get_registry():
            metadata = registry.get_registry()[key]
        else:
            raise Exception("Time series not registered with key: " + key)
        file_name = f"{metadata['values_column_name']}.parquet"
        return f"{self.storage_path}/NS={metadata['namespace']}/AS_OF={as_of.strftime('%Y%m%d%H%M%S')}/{file_name}"


class S3Persister(TimeSeriesDataPersister):
    def __init__(self, settings):
        self.bucket_name = settings.bucket_name
        self.storage_path = settings.storage_path

    def persist_data(self, key, as_of, data_frame):
        file_path = self._get_file_path(key, as_of)
        data_frame.to_parquet(f's3://{self.bucket_name}/{file_path}', index=False)

    def read_data(self, key, as_of):
        file_path = self._get_file_path(key, as_of)
        return pd.read_parquet(f's3://{self.bucket_name}/{file_path}')

    def delete_data(self, key, as_of):
        file_path = self._get_file_path(key, as_of)
        import boto3
        s3 = boto3.resource('s3')
        s3.Object(self.bucket_name, file_path).delete()

    def _get_file_path(self, key, as_of):
        if key in registry.get_registry():
            metadata = registry.get_registry()[key]
        else:
            raise Exception("Time series not registered with key: " + key)
        file_name = f"{metadata['values_column_name']}.parquet"
        return f"{self.storage_path}/NS={metadata['namespace']}/AS_OF={as_of.strftime('%Y%m%d%H%M%S')}/{file_name}"


class SQLDBPersister(TimeSeriesDataPersister):
    def __init__(self, settings):
        self.sql_connection = settings.sql_connection
        self.table_name = settings.table_name

    def persist_data(self, key, as_of, data_frame):
        data_frame['key'] = key
        data_frame['as_of'] = as_of
        data_frame.to_sql(self.table_name, self.sql_connection, if_exists='append')

    def read_data(self, key, as_of):
        query = f"SELECT * FROM {self.table_name} WHERE key = '{key}' AND as_of = '{as_of}'"
        return pd.read_sql(query, self.sql_connection)

    def delete_data(self, key, as_of):
        query = f"DELETE FROM {self.table_name} WHERE key = '{key}' AND as_of = '{as_of}'"
        # Execute the query using the SQL connection


