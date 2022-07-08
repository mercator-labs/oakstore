from __future__ import annotations

import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Literal
from typing import NamedTuple
from typing import overload

import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger('oakstore')

_DEFAULT_INDEX_NAME = 'DATE'
_DEFAULT_COLUMN_SCHEMA = {
    'OPEN': float,
    'HIGH': float,
    'LOW': float,
    'CLOSE': float,
    'VOLUME': int,
}
_DEFAULT_CHUNK_SIZE = 1_000_000


class _MetaData(NamedTuple):
    column_schema: dict[str, type]
    index_name: str


class OakStoreError(Exception):
    ...


class SchemaError(OakStoreError):
    ...


class Store:
    _base_path: Path
    _metadata: _MetaData
    _metadata_path: Path

    def __init__(
        self,
        base_path: Path | str,
        cols: dict[str, type] | None = None,
        index: str | None = None,
    ) -> None:
        # TODO handle cloud storage
        if isinstance(base_path, str):
            base_path = Path(base_path)
        self._base_path = base_path

        if not self._base_path.exists():
            self._base_path.mkdir(parents=True)

        if not (cols is None and index is None):
            if cols is None:
                cols = _DEFAULT_COLUMN_SCHEMA
            if index is None:
                index = _DEFAULT_INDEX_NAME
            _new_metadata = _MetaData(
                column_schema=cols,
                index_name=index,
            )
        else:
            _new_metadata = None

        self._metadata_path = self._base_path / 'metadata.pkl'
        if self._metadata_path.exists():
            with open(self._metadata_path, 'rb') as f:
                loaded_metadata = pickle.load(f)
            if _new_metadata is not None and loaded_metadata != _new_metadata:
                raise SchemaError(
                    f'loaded metadata {loaded_metadata!r} does not match '
                    f'provided metadata {_new_metadata!r}'
                )
            self._metadata = loaded_metadata

        else:
            if _new_metadata is None:
                _new_metadata = _MetaData(
                    column_schema=_DEFAULT_COLUMN_SCHEMA,
                    index_name=_DEFAULT_INDEX_NAME,
                )
            self._metadata = _new_metadata
            with open(self._metadata_path, 'wb') as f:
                pickle.dump(self._metadata, f)

    def _to_internal_type(self, *, data: pd.DataFrame) -> dd.DataFrame:
        def _schema_error() -> SchemaError:
            return SchemaError('data does not match store schema')

        data_cols_u = [c.upper() for c in data.columns.to_list()]
        schema_cols_u = [c.upper() for c in self._metadata.column_schema.keys()]

        if not all(c in data_cols_u for c in schema_cols_u):
            raise _schema_error()

        # create copy to not mutate original data
        data = data.copy()

        drop_list = [
            c for c in data.columns.to_list() if c.upper() not in schema_cols_u
        ]
        data.drop(columns=drop_list, inplace=True)

        rename_map = {c: c.upper() for c in data.columns.to_list()}
        data.rename(columns=rename_map, inplace=True)

        # rename columns and index
        data.index.name = self._metadata.index_name

        try:
            # change col types
            for col, tp in self._metadata.column_schema.items():
                data.astype({col: tp}, copy=False)
            # change index type
            data.index = pd.to_datetime(data.index)
        except (ValueError, TypeError):
            raise _schema_error()

        return dd.from_pandas(data, sort=True, chunksize=_DEFAULT_CHUNK_SIZE)

    def _from_internal_type(
        self, *, data: dd.DataFrame, raw_ddf: bool = False
    ) -> pd.DataFrame | dd.DataFrame:
        if raw_ddf:
            return data
        return data.compute()

    def _get_or_create_collection_path(
        self, *, collection: str, create: bool = False
    ) -> Path:
        collection_path = self._base_path / collection.upper()
        if create and not collection_path.exists():
            collection_path.mkdir(parents=True)
        return collection_path

    def _get_item_path(
        self, *, collection: str, item: str, create: bool = False
    ) -> Path:
        collection_path = self._get_or_create_collection_path(
            collection=collection, create=create
        )
        item_path = collection_path / item.upper()
        if create and not item_path.exists():
            item_path.mkdir(parents=True)
        return item_path

    def write(
        self,
        collection: str,
        item: str,
        *,
        data: pd.DataFrame,
        extend: bool = False,
    ) -> None:
        ddf = self._to_internal_type(data=data)

        item_path = self._get_item_path(collection=collection, item=item, create=False)
        if not extend and item_path.exists():
            raise ValueError(
                f'item {item.upper()} already exists in collection {collection.upper()}'
            )

        ddf.to_parquet(
            self._get_item_path(collection=collection, item=item, create=True),
            engine='pyarrow',
            compression='snappy',
            write_index=True,
        )

    def append(
        self,
        collection: str,
        item: str,
        *,
        data: pd.DataFrame,
    ) -> None:
        ddf = self._to_internal_type(data=data)

        item_path = self._get_item_path(collection=collection, item=item, create=False)
        if not item_path.exists():
            raise ValueError(
                f'item {item.upper()} does not exist in collection {collection.upper()}'
            )

        # append to existing data, keeping old records
        old_ddf = dd.read_parquet(
            item_path,
            engine='pyarrow',
        )
        # is this correct? should we use symmetric difference with keep=False and one more step?
        ddf = dd.concat([old_ddf, ddf]).drop_duplicates(keep='first')
        ddf.to_parquet(
            self._get_item_path(collection=collection, item=item, create=True),
            engine='pyarrow',
            compression='snappy',
            write_index=True,
        )

    @overload
    def query(
        self,
        collection: str,
        item: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        raw_ddf: Literal[False],
    ) -> pd.DataFrame:
        ...

    @overload
    def query(
        self,
        collection: str,
        item: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        raw_ddf: Literal[True],
    ) -> dd.DataFrame:
        ...

    def query(
        self,
        collection: str,
        item: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        raw_ddf: bool = False,
    ) -> pd.DataFrame | dd.DataFrame:

        item_path = self._get_item_path(collection=collection, item=item, create=False)
        if not item_path.exists():
            raise ValueError(
                f'item {item.upper()} does not exist in collection {collection.upper()}'
            )

        if start is None:
            start = datetime(1678, 1, 1)  # lowest possible year supported by pandas
        if end is None:
            end = datetime.now()

        parquet_filters = [
            (self._metadata.index_name, '>=', start),
            (self._metadata.index_name, '<=', end),
        ]

        ddf = dd.read_parquet(
            item_path,
            engine='pyarrow',
            filters=parquet_filters,
        )

        sliced_ddf: dd.DataFrame = ddf.loc[(start <= ddf.index) & (end >= ddf.index)]
        return self._from_internal_type(data=sliced_ddf, raw_ddf=raw_ddf)
