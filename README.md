[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/mercator-labs/oakstore/main.svg)](https://results.pre-commit.ci/latest/github/mercator-labs/oakstore/main)

# oakstore

highspeed timeseries pandas dataframe database

```python
import pandas as pd
from oakstore import Store
from datetime import datetime

store = Store(base_path='./data')

df = pd.DataFrame(...)  # dataframe with timestamp as index
schema = {'a': float, 'b': float, 'c': int}  # and columns according to this schema

# inital write
store.write('SOME','KEY', data=df)
...

df = store.query('SOME', 'KEY')  # get full history
#  or query depending on timestamp index
df = store.query('SOME', 'KEY', start=datetime(2020, 1, 1), end=datetime(2021, 1, 1))
...

# appending (this will only append new data and will not overwrite old data, drops any duplicates)
store.append('SOME', 'KEY', data=df)
```
