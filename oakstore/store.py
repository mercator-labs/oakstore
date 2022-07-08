from __future__ import annotations

import logging
import os
import pickle
import re
from datetime import datetime
from pathlib import Path
from typing import cast
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
_KEY_REGEX = re.compile(r'^[-a-zA-Z0-9_.]+\Z')
_ITEMS_DIR = 'items'


class _MetaData(NamedTuple):
    column_schema: dict[str, type]
    index_name: str


class OakStoreError(Exception):
    ...


class SchemaError(OakStoreError):
    ...


class ItemKeyError(OakStoreError):
    ...


class _Item:
    _key: str
    _store: Store

    def __init__(self, *, key: str, store: Store) -> None:
        self._key = key
        self._store = store

    def __iadd__(self, data: pd.DataFrame) -> _Item:
        self._store._append(key=self._key, data=data)
        return self

    def __getitem__(self, sl: slice) -> pd.DataFrame:
        if sl.step is not None:
            raise KeyError('step not supported')
        if sl.start is not None and not isinstance(sl.start, datetime):
            raise KeyError('start must be a datetime')
        if sl.stop is not None and not isinstance(sl.stop, datetime):
            raise KeyError('stop must be a datetime')
        return self._store._query(key=self._key, start=sl.start, end=sl.stop)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(key={self._key!r}, store={self._store!r})'


class Store:
    _base_path: Path
    _items_path: Path
    _metadata: _MetaData
    _metadata_path: Path

    def __init__(
        self,
        base_path: Path | str = './data',
        cols: dict[str, type] | None = None,
        index: str | None = None,
    ) -> None:
        # TODO handle cloud storage
        if isinstance(base_path, str):
            base_path = Path(base_path)
        self._base_path = base_path
        if not self._base_path.exists():
            self._base_path.mkdir(parents=True)

        self._items_path = self._base_path / _ITEMS_DIR
        if not self._items_path.exists():
            self._items_path.mkdir(parents=True)

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

    def __repr__(self) -> str:
        return f'{type(self).__name__}(base_path={self._base_path!r})'

    def __getitem__(self, key: str) -> _Item:
        return _Item(key=key, store=self)

    def __setitem__(self, key: str, data: pd.DataFrame | _Item) -> None:
        if isinstance(data, _Item):
            return
        self._write(key=key, data=data)

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

    @staticmethod
    def _validate_key(key: str) -> bool:
        return _KEY_REGEX.match(key) is not None

    def _get_item_path(self, *, key: str, create: bool = False) -> Path:
        if not self._validate_key(key):
            raise ItemKeyError(f'invalid key {key.upper()}')

        item_path = self._items_path / key.upper()
        if create and not item_path.exists():
            item_path.mkdir(parents=True)
        return item_path

    def _write(
        self,
        key: str,
        *,
        data: pd.DataFrame,
    ) -> None:
        item_path = self._get_item_path(key=key, create=False)
        if item_path.exists():
            raise ValueError(f'key={key.upper()} already exists')

        ddf = self._to_internal_type(data=data)
        ddf.to_parquet(
            self._get_item_path(key=key, create=True),
            engine='pyarrow',
            compression='snappy',
            write_index=True,
        )

    def _append(
        self,
        key: str,
        *,
        data: pd.DataFrame,
    ) -> None:
        item_path = self._get_item_path(key=key, create=False)
        if not item_path.exists():
            raise ValueError(f'key={key.upper()} does not exist, (use write instead?)')

        ddf = self._to_internal_type(data=data)
        old_ddf = dd.read_parquet(
            item_path,
            engine='pyarrow',
        )
        # is this correct? should we use symmetric difference with keep=False and one more step?
        ddf = dd.concat([old_ddf, ddf]).drop_duplicates(keep='first')
        ddf.to_parquet(
            self._get_item_path(key=key, create=True),
            engine='pyarrow',
            compression='snappy',
            write_index=True,
        )

    def _query(
        self,
        key: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame | dd.DataFrame:

        item_path = self._get_item_path(key=key, create=False)
        if not item_path.exists():
            raise ValueError(f'key={key.upper()} does not exist')

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
        return self._from_internal_type(data=sliced_ddf, raw_ddf=False)
