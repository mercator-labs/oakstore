from __future__ import annotations

import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

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


class Store:
    _base_path: Path
    _metadata: _MetaData
    _metadata_path: Path

    def __init__(
        self,
        *,
        base_path: Path | str | None = None,
        cols: dict[str, type] | None = None,
        index: str | None = None,
    ) -> None:
        if base_path is None:
            try:
                base_path = Path(os.environ['OAKSTORE_BASE_PATH'])
            except KeyError:
                raise ValueError(
                    'no base path was provided, either set $OAKSTORE_BASE_PATH or pass base_path to DataStore'
                )
        elif isinstance(base_path, str):
            base_path = Path(base_path)

        self._base_path = base_path

        if not self._base_path.exists():
            self._base_path.mkdir(parents=True)

        self._metadata_path = self._base_path / 'metadata.pkl'
        if self._metadata_path.exists():
            with open(self._metadata_path, 'rb') as f:
                self._metadata = pickle.load(f)
        else:
            if cols is None:
                cols = _DEFAULT_COLUMN_SCHEMA
            if index is None:
                index = _DEFAULT_INDEX_NAME

            self._metadata = _MetaData(
                column_schema=cols,
                index_name=index,
            )
            with open(self._metadata_path, 'wb') as f:
                pickle.dump(self._metadata, f)

    def _to_internal_type(self, data: pd.DataFrame) -> pd.DataFrame:
        # check schema compatibility
        old_cols: list[str] = data.columns.to_list()
        old_cols_u = [c.upper() for c in old_cols]
        new_cols_u = [c.upper() for c in self._metadata.column_schema.keys()]
        if old_cols_u != new_cols_u:
            raise ValueError(
                'columns in dataframe do not match schema: '
                f'{old_cols_u} != {new_cols_u}'
            )

        # rename columns and index
        col_map = {old: new for old, new in zip(old_cols, new_cols_u)}
        data.rename(columns=col_map, inplace=True)
        data.index.name = self._metadata.index_name

        # cast types
        for col, tp in self._metadata.column_schema.items():
            data = data.astype({col: tp})
        data.index = data.index.astype(int)
        return data

    def _from_internal_type(self, data: pd.DataFrame) -> pd.DataFrame:
        data.index = pd.to_datetime(data.index)
        return data

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
        data = self._to_internal_type(data)
        ddf = dd.from_pandas(data, sort=True, chunksize=_DEFAULT_CHUNK_SIZE)

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
        data = self._to_internal_type(data)
        ddf = dd.from_pandas(data, sort=True, chunksize=_DEFAULT_CHUNK_SIZE)

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

    def query(
        self,
        collection: str,
        item: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dd.DataFrame:

        item_path = self._get_item_path(collection=collection, item=item, create=False)
        if not item_path.exists():
            raise ValueError(
                f'item {item.upper()} does not exist in collection {collection.upper()}'
            )

        if start is None:
            start = datetime(1678, 1, 1)  # lowest possible year supported by pandas
        if end is None:
            end = datetime.now()

        # datetimes are stored as int internally
        _start = pd.Timestamp(start).value
        _end = pd.Timestamp(end).value
        parquet_filters = [
            (self._metadata.index_name, '>=', _start),
            (self._metadata.index_name, '<=', _end),
        ]

        ddf = dd.read_parquet(
            item_path,
            engine='pyarrow',
            filters=parquet_filters,
        )

        # temporary fix
        ddf = ddf.compute()
        sliced_ddf = ddf[(_start <= ddf.index) & (_end >= ddf.index)]
        return self._from_internal_type(sliced_ddf)  # type: ignore
