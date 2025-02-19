"""
Создание csv с признаком был ли бар профитным.
DeepSeek часть кода.
Работает быстрей.
Проверить данные.
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view


def determine_trade_results(df, point):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').reset_index(drop=True)

    df['direction'] = np.where(df['close'] > df['open'], 'up', 'down')

    mask_up = df['direction'] == 'up'
    df['takeprofit'] = np.where(
        mask_up,
        df['close'] + df['size'] + point,
        df['close'] - df['size'] - point
    )
    df['stoploss'] = np.where(
        mask_up,
        df['close'] - df['size'] - point,
        df['close'] + df['size'] + point
    )

    high_vals = df['high'].values
    low_vals = df['low'].values
    window_size = 300

    # Создаем скользящие окна для high и low
    if len(df) < window_size:
        raise ValueError("DataFrame is too short for the specified window size.")

    high_windows = sliding_window_view(high_vals, window_size)
    low_windows = sliding_window_view(low_vals, window_size)

    n_valid = len(high_windows)
    indices = np.arange(n_valid)
    direction_valid = df['direction'].iloc[indices].values

    up_mask = direction_valid == 'up'
    up_indices = indices[up_mask]
    down_indices = indices[~up_mask]

    tp_hit = np.full(n_valid, window_size)
    sl_hit = np.full(n_valid, window_size)

    # Обработка 'up' направления
    if up_indices.size > 0:
        up_high = high_windows[up_indices]
        up_low = low_windows[up_indices]
        up_tp = df['takeprofit'].values[up_indices][:, None]
        up_sl = df['stoploss'].values[up_indices][:, None]

        tp_cond = up_high >= up_tp
        sl_cond = up_low <= up_sl

        tp_any = tp_cond.any(axis=1)
        sl_any = sl_cond.any(axis=1)

        tp_idx = np.where(tp_any, tp_cond.argmax(axis=1), window_size)
        sl_idx = np.where(sl_any, sl_cond.argmax(axis=1), window_size)

        tp_hit[up_indices] = tp_idx
        sl_hit[up_indices] = sl_idx

    # Обработка 'down' направления
    if down_indices.size > 0:
        down_high = high_windows[down_indices]
        down_low = low_windows[down_indices]
        down_tp = df['takeprofit'].values[down_indices][:, None]
        down_sl = df['stoploss'].values[down_indices][:, None]

        tp_cond = down_low <= down_tp
        sl_cond = down_high >= down_sl

        tp_any = tp_cond.any(axis=1)
        sl_any = sl_cond.any(axis=1)

        tp_idx = np.where(tp_any, tp_cond.argmax(axis=1), window_size)
        sl_idx = np.where(sl_any, sl_cond.argmax(axis=1), window_size)

        tp_hit[down_indices] = tp_idx
        sl_hit[down_indices] = sl_idx

    profitable = np.select(
        [
            (tp_hit < sl_hit) & (tp_hit < window_size),
            (sl_hit < tp_hit) & (sl_hit < window_size),
            ((tp_hit >= window_size) & (sl_hit >= window_size)) | (
                        (tp_hit == sl_hit) & (tp_hit < window_size))
        ],
        [
            1,
            -1,
            np.nan
        ],
        default=np.nan
    )

    df['profitable'] = pd.Series(profitable, index=df.index[:n_valid]).reindex(df.index,
                                                                               fill_value=np.nan)
    df = df.dropna(subset=['profitable']).reset_index(drop=True)
    df['profitable'] = df['profitable'].astype(int)

    return df


if __name__ == '__main__':
    point = 10
    db_path = Path(r'C:\Users\Alkor\gd\data_quote_db\RTS_Range.db')

    conn = sqlite3.connect(db_path)
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    table = pd.read_sql_query(query, conn).iloc[0, 0]
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Оптимизированная фильтрация по времени
    df['datetime'] = pd.to_datetime(df['datetime'])
    exclude_times = {
        pd.Timestamp('10:00:00').time(),
        pd.Timestamp('10:00:01').time(),
        pd.Timestamp('19:00:00').time(),
        pd.Timestamp('19:00:01').time(),
        pd.Timestamp('19:05:00').time(),
        pd.Timestamp('19:05:01').time()
    }
    df = df[~df['datetime'].dt.time.isin(exclude_times)].reset_index(drop=True)

    df = determine_trade_results(df, point)

    # Настройки для отображения широкого df pandas
    pd.options.display.width = 1200
    pd.options.display.max_colwidth = 100
    pd.options.display.max_columns = 100
    # Проверьте данные
    print(df)

    df.to_csv("result_03.csv", index=False)
    print(df['profitable'].value_counts())