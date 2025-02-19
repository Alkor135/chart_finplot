"""
Создание csv с признаком был ли бар профитным.
Открытие позиции по close текущего бара.
Ошибка в определении профитности бара.
"""
import sys
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd


def determine_trade_results(df, point):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').reset_index(drop=True)

    df['direction'] = np.where(df['close'] > df['open'], 'up', 'down')

    df['takeprofit'] = np.where(
        df['direction'] == 'up',
        df['close'] + df['size'] + point,
        df['close'] - df['size'] - point
    )
    df['stoploss'] = np.where(
        df['direction'] == 'up',
        df['close'] - df['size'] - point,
        df['close'] + df['size'] + point
    )

    high_arr = df['high'].to_numpy()
    low_arr = df['low'].to_numpy()
    tp_arr = df['takeprofit'].to_numpy()
    sl_arr = df['stoploss'].to_numpy()
    direction_arr = df['direction'].to_numpy()

    results = np.full(len(df), np.nan)

    for i in range(len(df) - 300):
        if i % 1000 == 0:
            sys.stdout.write(f'\rProcessing: {round((i / len(df)) * 100, 2)}%')
            sys.stdout.flush()

        future_highs = high_arr[i + 1:i + 301]
        future_lows = low_arr[i + 1:i + 301]

        if direction_arr[i] == 'up':
            sl_hit = np.argmax(future_lows <= sl_arr[i])
            tp_hit = np.argmax(future_highs >= tp_arr[i])
        else:
            sl_hit = np.argmax(future_highs >= sl_arr[i])
            tp_hit = np.argmax(future_lows <= tp_arr[i])

        if sl_hit == 0 and tp_hit == 0:
            results[i] = np.nan
        elif tp_hit and (tp_hit < sl_hit or not sl_hit):
            results[i] = 1
        elif sl_hit and (sl_hit < tp_hit or not tp_hit):
            results[i] = -1

    df['profitable'] = results
    df = df.dropna(subset=['profitable']).reset_index(drop=True)
    df['profitable'] = df['profitable'].astype(int)
    print()

    return df


if __name__ == '__main__':
    point = 10
    db_path = Path(r'C:\Users\Alkor\gd\data_quote_db\RTS_Range.db')

    conn = sqlite3.connect(db_path)
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    table = pd.read_sql_query(query, conn).iloc[0, 0]

    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()

    pattern = r'(?:10:00:00|10:00:01|19:00:00|19:00:01|19:05:00|19:05:01)'
    df = df[~df['datetime'].str.contains(pattern, regex=True)].reset_index(drop=True)

    df = determine_trade_results(df, point)

    pd.options.display.width = 1200
    pd.options.display.max_colwidth = 100
    pd.options.display.max_columns = 100
    print(df)

    df.to_csv("result.csv", index=False)

    profit_stats = df['profitable'].value_counts()
    print(profit_stats)
