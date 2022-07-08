from __future__ import annotations

import tempfile
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

import pytest

from oakstore import Store
from oakstore.store import _DEFAULT_COLUMN_SCHEMA
from oakstore.store import _DEFAULT_INDEX_NAME
from oakstore.store import _ITEMS_DIR
from oakstore.store import _MetaData
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
    assert (base_path / 'metadata.pkl').is_file()


def test_write(store, msft_data):
    store.write('MSFT', data=msft_data)
    assert (store._base_path / _ITEMS_DIR / 'MSFT').exists()
    assert (store.query('MSFT') == msft_data).all


def test_query(store, msft_data):
    store.write('MSFT', data=msft_data)

    # these should exist
    assert not store.query('MSFT', start=datetime(2000, 1, 1)).empty
    assert not store.query('MSFT', end=datetime(2000, 1, 1)).empty
    assert not store.query(
        'MSFT', start=datetime(2000, 1, 1), end=datetime(2020, 1, 1)
    ).empty

    # doesnt exist
    assert store.query('MSFT', start=datetime(2100, 1, 1)).empty


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
        store.write(key, data=msft_data)
