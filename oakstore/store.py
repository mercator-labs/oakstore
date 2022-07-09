from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from types import TracebackType

import dask.dataframe as dd
import pandas as pd

from oakstore.excpetions import ItemKeyError
from oakstore.excpetions import SchemaError

logger = logging.getLogger('oakstore')

_DEFAULT_CHUNK_SIZE = 1_000_000
_KEY_REGEX = re.compile(r'^[-a-zA-Z0-9_.]+\Z')

_DEFAULT_SCHEMA = pd.DataFrame(
    columns=['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'],
    dtype=[float, float, float, float, int],
    index=pd.DatetimeIndex(name='DATE'),
)


def _schema_valid(schema: pd.DataFrame) -> bool:
    if len(schema) != 0:
        return False
    if not isinstance(schema.index, pd.DatetimeIndex):
        return False
    return True


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
    _schema: pd.DataFrame

    def __init__(
        self,
        base_path: Path | str = './data',
        schema: pd.DataFrame | None = None,
    ) -> None:
        if isinstance(base_path, str):
            base_path = Path(base_path)
        self._base_path = base_path
        if not self._base_path.exists():
            self._base_path.mkdir(parents=True)

        if schema is not None:
            if not _schema_valid(schema):
                raise SchemaError('schema is not valid')

        if schema is None:
            schema = _DEFAULT_SCHEMA
        self._schema = schema

    def __repr__(self) -> str:
        return f'{type(self).__name__}(base_path={self._base_path!r})'

    def __getitem__(self, key: str) -> _Item:
        return _Item(key=key, store=self)

    def __setitem__(self, key: str, data: pd.DataFrame | _Item) -> None:
        if isinstance(data, _Item):
            return
        self._write(key=key, data=data)

    def __enter__(self) -> Store:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    def _to_internal_type(self, *, data: pd.DataFrame) -> dd.DataFrame:
        if not isinstance(data.index, pd.DatetimeIndex):
            raise SchemaError('index is not a datetime index')
        if data.columns != self._schema.columns:
            raise SchemaError('columns of data do not match schema')
        if data.dtypes != self._schema.dtypes:
            raise SchemaError('dtypes of data do not match schema')
        if data.index.name != self._schema.index.name:
            raise SchemaError('index name of data does not match schema')

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
            (self._schema.index.name, '>=', start),
            (self._schema.index.name, '<=', end),
        ]

        ddf = dd.read_parquet(
            item_path,
            engine='pyarrow',
            filters=parquet_filters,
        )

        sliced_ddf: dd.DataFrame = ddf.loc[(start <= ddf.index) & (end >= ddf.index)]
        return self._from_internal_type(data=sliced_ddf, raw_ddf=False)
