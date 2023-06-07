from abc import ABC, abstractmethod
import pandas as pd

import registry

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
    def create_persister(persister_type, **kwargs):
        if persister_type == 'local_file':
            return LocalFilePersister(**kwargs)
        elif persister_type == 's3':
            return S3Persister(**kwargs)
        elif persister_type == 'sql_db':
            return SQLDBPersister(**kwargs)
        else:
            raise ValueError(f"Unknown persister type: {persister_type}")

class LocalFilePersister(TimeSeriesDataPersister):
    def __init__(self, storage_path):
        self.storage_path = storage_path

    def persist_data(self, key, as_of, data_frame):
        file_path = self._get_file_path(key, as_of)
        data_frame.to_parquet(file_path, index=False)

    def read_data(self, key, as_of):
        file_path = self._get_file_path(key, as_of)
        return pd.read_parquet(file_path)

    def delete_data(self, key, as_of):
        file_path = self._get_file_path(key, as_of)
        import os
        os.remove(file_path)

    def _get_file_path(self, key, as_of):
        metadata = registry.get_registry()[key]
        file_name = f"{metadata['values_column_name']}.parquet"
        return f"{self.storage_path}/NS={metadata['namespace']}/AS_OF={as_of.strftime('%Y%m%d%H%M%S')}/{file_name}"


class S3Persister(TimeSeriesDataPersister):
    def __init__(self, bucket_name, storage_path):
        self.bucket_name = bucket_name
        self.storage_path = storage_path

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
        metadata = registry.get_registry()[key]
        file_name = f"{metadata['values_column_name']}.parquet"
        return f"{self.storage_path}/NS={metadata['namespace']}/AS_OF={as_of.strftime('%Y%m%d%H%M%S')}/{file_name}"


class SQLDBPersister(TimeSeriesDataPersister):
    def __init__(self, sql_connection, table_name):
        self.sql_connection = sql_connection
        self.table_name = table_name

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


