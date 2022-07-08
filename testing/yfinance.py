from __future__ import annotations

import yfinance as yf


def get_msft():
    df = yf.download('MSFT', period='max')
    df.drop(columns=['Adj Close'], inplace=True)
    df.rename(
        columns={
            'Open': 'OPEN',
            'High': 'HIGH',
            'Low': 'LOW',
            'Close': 'CLOSE',
            'Volume': 'VOLUME',
        },
        inplace=True,
    )
    return df
