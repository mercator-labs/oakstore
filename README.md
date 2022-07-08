[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![tests](https://github.com/mercator-labs/oakstore/actions/workflows/tests.yaml/badge.svg)](https://github.com/mercator-labs/oakstore/actions/workflows/tests.yaml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/mercator-labs/oakstore/main.svg)](https://results.pre-commit.ci/latest/github/mercator-labs/oakstore/main)

# oakstore

highspeed timeseries pandas dataframe database

## usage

here is a quick rundown of things you can do with oakstore

```python
>>> import pandas as pd
>>> from oakstore import Store
>>> from datetime import datetime

>>> store = Store(base_path='./data')

# lets load some end of day OHLCV data, custom dataschema is also supported
>>> df = pd.DataFrame(...)
>>> df
                  OPEN        HIGH         LOW       CLOSE      VOLUME
DATE
1986-03-13    0.088542    0.101563    0.088542    0.097222  1031788800
1986-03-14    0.097222    0.102431    0.097222    0.100694   308160000
1986-03-17    0.100694    0.103299    0.100694    0.102431   133171200
1986-03-18    0.102431    0.103299    0.098958    0.099826    67766400
1986-03-19    0.099826    0.100694    0.097222    0.098090    47894400
...                ...         ...         ...         ...         ...
2022-07-01  256.390015  259.769989  254.610001  259.579987    22825200
2022-07-05  256.160004  262.980011  254.740005  262.850006    22941000
2022-07-06  263.750000  267.989990  262.399994  266.209991    23824400
2022-07-07  265.119995  269.059998  265.019989  268.399994    20848100
2022-07-08  264.790009  268.019989  263.285004  267.071686    12472863

[9155 rows x 5 columns]

>>> # inital write
>>> store['KEY'] = df

# access full history
>>> df = store['KEY'][:]
# or only a slice, you can slice billions of rows with this
>>> df = store['KEY'][datetime(2020, 1, 1) : datetime(2021, 1, 1)]

>>> # appending (this will only append new data and will not overwrite old data, drops any duplicates)
>>> store['KEY'] += df
```

## install

```
pip install git+ssh@github.com/mercator-labs/oakstore
```

### dependencies

you will also have to install [`snappy`](https://github.com/andrix/python-snappy#build--install), best just follow their installation guide.
