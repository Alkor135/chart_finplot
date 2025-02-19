import sqlite3
from pathlib import Path
from datetime import timezone

import pandas as pd
import finplot as fplt

fplt.display_timezone = timezone.utc  # Настройка тайм зоны, чтобы не было смещения времени


def chart_range(df):
    # Меняем тип данных в колонке
    df['datetime'] = pd.to_datetime(df['datetime'])

    # create two axes
    ax = fplt.create_plot('RTS', rows=1)
    ax.set_visible(xgrid=True, ygrid=True)

    # plot candle sticks
    # candles = df[['open', 'close', 'high', 'low']]  # Время в индексе
    candles = df[['datetime', 'open', 'close', 'high', 'low']]  # Время не в индексе
    fplt.candlestick_ochl(candles, ax=ax)

    # overlay volume on the top plot
    # volumes = df[['open','close','volume']]  # Время в индексе
    volumes = df[['datetime', 'open', 'close', 'volume']]  # Время не в индексе
    fplt.volume_ocv(volumes, ax=ax.overlay())

    fplt.show()

if __name__ == '__main__':
    # Укажите путь к вашей базе данных SQLite
    db_path = Path(r'C:\Users\Alkor\gd\data_quote_db\RTS_Range.db')

    # Установите соединение с базой данных
    conn = sqlite3.connect(db_path)

    query = "SELECT name FROM sqlite_master WHERE type='table'"
    table = pd.read_sql_query(query, conn).iloc[0, 0]
    # print(table)

    # Выполните SQL-запрос и загрузите результаты в DataFrame
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)

    # Закройте соединение
    conn.close()

    # Фильтрация df
    # Регулярное выражение с незахватывающей группой (?:)
    pattern = r'(?:10:00:00|10:00:01|19:00:00|19:00:01|19:05:00|19:05:01)'
    # Фильтрация строк (удаляем строки с совпадением)
    df = df[~df['datetime'].str.contains(pattern, regex=True)]
    # Сброс индекса (переиндексация)
    df = df.reset_index(drop=True)

    # Проверьте данные
    print(df)

    chart_range(df)
