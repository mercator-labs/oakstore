from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yfinance as yf

from oakstore import Store

STORE_PATH = Path(__file__).parent / 'data'

ds = Store(base_path=STORE_PATH)

df = yf.download('MSFT', period='max')
df.drop(columns=['Adj Close'], inplace=True)
names = df.columns.tolist()
new_names_map = {
    old: new for old, new in zip(names, ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
}
df.rename(columns=new_names_map, inplace=True)
df.index.name = 'DATE'

print('before', df)
# ds.write('DATA', 'MSFT', df)

df = ds.query('DATA', 'MSFT', start=datetime(2020, 1, 1), end=datetime(2020, 2, 1))
print('after', df)
