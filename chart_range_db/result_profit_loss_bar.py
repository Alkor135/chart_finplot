"""
Создание csv с признаком был ли бар профитным.
Создание новых колонок: direction, takeprofit, stoploss, profitable.
Открытие позиции по close текущего бара.
"""

import sys
import sqlite3
from pathlib import Path
import numpy as np

import pandas as pd
import zipfile


def determine_trade_results(df, point):
    """
    Функция для определения результата сделки и создания новых колонок:
    direction, takeprofit, stoploss, profitable.
    """
    # Предварительная обработка данных
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').reset_index(drop=True)

    # Определяем направление текущего бара
    df['direction'] = np.where(df['close'] > df['open'], 'up', 'down')

    # Рассчитываем takeprofit/stoploss для каждого бара
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

    def check_profit_loss(index):
        """ Функция для проверки достижения takeprofit и stoploss """
        # Вывод процента обработки
        if index % 1000 == 0:
            sys.stdout.write(f'\rProcessing: {round((index / len(df)) * 100, 2)}%')
            sys.stdout.flush()

        # Если значения takeprofit или stoploss в текущей строке (index) неопределены (NaN)
        if pd.isna(df.loc[index, 'takeprofit']) or pd.isna(df.loc[index, 'stoploss']):
            # Возвращаем NaN для колонки "profitable"
            return np.nan

        # Срез будущих баров для поиска, что было первым профит или лосс.
        future_bars = df.iloc[index + 1:index + 301]

        if df.loc[index, 'direction'] == 'up':
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

    # Получение списка таблиц
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

    # Добавление новых колонок: direction, takeprofit, stoploss, profitable
    df = determine_trade_results(df, point)

    # Настройки для отображения широкого df pandas
    pd.options.display.width = 1200
    pd.options.display.max_colwidth = 100
    pd.options.display.max_columns = 100
    # Проверьте данные
    print(df)

    # # Сохранение в файл
    # df.to_csv("result.csv", index=False)

    # Указываем путь к ZIP-файлу и имени файла внутри архива
    zip_filename = r"C:\Users\Alkor\gd\data_quote_zip\RTS_range.zip"
    csv_filename = "RTS_range.csv"
    # Сохраняем DataFrame в ZIP
    with zipfile.ZipFile(zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with zf.open(csv_filename, mode="w") as buffer:
            df.to_csv(buffer, index=False)

    profit_stats = df['profitable'].value_counts()
    print(profit_stats)
