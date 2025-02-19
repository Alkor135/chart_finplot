"""
Создание csv с признаком был ли бар профитным.
Открытие позиции по open следующего бара.
Лучше не использовать.
"""

import sys
import sqlite3
from pathlib import Path
import numpy as np

import pandas as pd


def determine_trade_results(df, point):
    """ Векторизированная функция для определения результата сделки """

    # Определение направления предыдущего бара
    df['prev_direction'] = np.where(df['close'].shift(1) > df['open'].shift(1), 'up', 'down')

    # Расчет takeprofit и stoploss для каждого бара
    df['takeprofit'] = np.where(
        df['prev_direction'] == 'up',
        df['open'] + (df['size'] + point),
        np.where(df['prev_direction'] == 'down', df['open'] - (df['size'] + point), np.nan)
    )

    df['stoploss'] = np.where(
        df['prev_direction'] == 'up',
        df['open'] - (df['size'] + point),
        np.where(df['prev_direction'] == 'down', df['open'] + (df['size'] + point), np.nan)
    )

    def check_profit_loss(index):
        """ Функция для проверки достижения takeprofit и stoploss """
        # Вывод процента обработки
        sys.stdout.write(f'\rProcessing: {round(((index / len(df)) * 100), 2)}%')
        sys.stdout.flush()

        if pd.isna(df.loc[index, 'takeprofit']) or pd.isna(df.loc[index, 'stoploss']):
            return np.nan

        future_bars = df.iloc[index + 1:index + 301]

        if df.loc[index, 'prev_direction'] == 'up':
            sl_hit = (future_bars['low'] <= df.loc[index, 'stoploss']).idxmax()
            tp_hit = (future_bars['high'] >= df.loc[index, 'takeprofit']).idxmax()
        else:
            sl_hit = (future_bars['high'] >= df.loc[index, 'stoploss']).idxmax()
            tp_hit = (future_bars['low'] <= df.loc[index, 'takeprofit']).idxmax()

        if tp_hit == 0 and sl_hit == 0:
            return np.nan
        elif tp_hit < sl_hit:
            return 1
        elif tp_hit > sl_hit:
            return -1
        else:
            return np.nan

    # Применение функции ко всем барам
    df['profitable'] = [check_profit_loss(i) for i in range(len(df) - 300)] + [np.nan] * 300
    # Удаляем строк с NaN
    df = df.dropna(subset=['profitable'])
    # Сброс индекса (переиндексация)
    df = df.reset_index(drop=True)
    # Преобразуем в числовой формат
    df.loc[:, 'profitable'] = df['profitable'].astype(int)
    print()

    return df


if __name__ == '__main__':
    # Тик пунктов
    point = 10
    # Укажите путь к вашей базе данных SQLite
    db_path = Path(r'C:\Users\Alkor\gd\data_quote_db\RTS_Range.db')
    # ---------------------------------------------------------------------------------------------
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

    # Добавление новой колонки с результатом сделки
    df = determine_trade_results(df, point)

    # Настройки для отображения широкого df pandas
    pd.options.display.width = 1200
    pd.options.display.max_colwidth = 100
    pd.options.display.max_columns = 100
    # Проверьте данные
    print(df)

    # Сохранение в файл
    df.to_csv("result.csv", index=False)

    profit_stats = df['profitable'].value_counts()
    print(profit_stats)
