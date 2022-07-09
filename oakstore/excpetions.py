from __future__ import annotations


class OakStoreError(Exception):
    ...


class SchemaError(OakStoreError):
    ...


class ItemKeyError(OakStoreError):
    ...


__all__ = [
    'OakStoreError',
    'SchemaError',
    'ItemKeyError',
]
