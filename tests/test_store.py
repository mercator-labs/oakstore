from __future__ import annotations

import tempfile
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

import pytest

from oakstore import Store
from oakstore.store import _DEFAULT_COLUMN_SCHEMA  # type: ignore
from oakstore.store import _DEFAULT_INDEX_NAME  # type: ignore
from oakstore.store import _ITEMS_DIR  # type: ignore
from oakstore.store import _MetaData  # type: ignore
from oakstore.store import ItemKeyError
from testing.yfinance import get_msft


@pytest.fixture(scope='session')
def msft_data():
    return get_msft()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory(prefix='oakstore_tests') as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def store(temp_dir):
    return Store(base_path=temp_dir / 'store')


def test_create_store(temp_dir):
    base_path = temp_dir / 'store'
    store = Store(base_path=base_path)
    assert store._metadata == _MetaData(
        index_name=_DEFAULT_INDEX_NAME,
        column_schema=_DEFAULT_COLUMN_SCHEMA,
    )
    assert base_path.is_dir()
    assert (base_path / _ITEMS_DIR).is_dir()
    assert (base_path / 'metadata.pkl').is_file()


def test_write(store, msft_data):
    store['MSFT'] = msft_data
    assert (store._items_path / 'MSFT').exists()
    assert (store['MSFT'][:] == msft_data).all


def test_query(store, msft_data):
    store['MSFT'] = msft_data

    # these should exist
    assert not store['MSFT'][datetime(2000, 1, 1) :].empty
    assert not store['MSFT'][: datetime(2000, 1, 1)].empty
    assert not store['MSFT'][datetime(2000, 1, 1) : datetime(2020, 1, 1)].empty

    # doesnt exist
    assert store['MSFT'][datetime(2100, 1, 1) :].empty


def test_append(store, msft_data):
    store['MSFT'] = msft_data.iloc[:10]
    store['MSFT'] += msft_data
    assert (store['MSFT'][:] == msft_data).all


@pytest.mark.parametrize(
    ('key', 'exc'),
    [
        pytest.param('MSFT', nullcontext(), id='valid'),
        pytest.param('foo bar', pytest.raises(ItemKeyError), id='spaces'),
        pytest.param('*foo', pytest.raises(ItemKeyError), id='star'),
        pytest.param('#foo', pytest.raises(ItemKeyError), id='hashtag'),
    ],
)
def test_invalid_key(store, msft_data, key, exc):
    with exc:
        store[key] = msft_data
