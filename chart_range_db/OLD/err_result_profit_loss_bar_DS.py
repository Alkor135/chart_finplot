"""
Создание csv с признаком был ли бар профитным.
DeepSeek часть кода.
Работает быстрей.
Сомнительные данные.
"""
import sqlite3
from pathlib import Path
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
import pandas as pd


def determine_trade_results(df, point):
    """ Векторизированная функция для определения результата сделки """
    # Предварительная обработка данных
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime').reset_index(drop=True)

    # 1. Определяем направление предыдущего бара
    df['prev_direction'] = np.where(df['close'].shift(1) > df['open'].shift(1), 'up', 'down')

    # 2. Рассчитываем takeprofit/stoploss для каждого бара
    df['takeprofit'] = np.where(
        df['prev_direction'] == 'up',
        df['open'] + df['size'] + point,
        df['open'] - df['size'] - point
    )
    df['stoploss'] = np.where(
        df['prev_direction'] == 'up',
        df['open'] - df['size'] - point,
        df['open'] + df['size'] + point
    )

    # 3. Создаем скользящие окна для анализа следующих 300 баров
    window_size = 300
    if len(df) < window_size:
        raise ValueError("Данных меньше чем размер окна (300 баров)")

    high = df['high'].values
    low = df['low'].values

    # Создаем матрицу скользящих окон
    high_windows = sliding_window_view(high, window_size)
    low_windows = sliding_window_view(low, window_size)

    # Создаем маску для валидных окон
    n_windows = len(high_windows)
    valid_indices = np.arange(len(df))[:n_windows]  # Только индексы с валидными окнами

    # 4. Векторизованный расчет прибыльности
    def calculate_profitability(tp, sl, direction, high_win, low_win):
        # Инициализируем массивы
        tp_hits = np.zeros(len(tp), dtype=int)
        sl_hits = np.zeros(len(sl), dtype=int)

        buy_mask = (direction == 'up')
        sell_mask = ~buy_mask

        # Обработка покупок
        if buy_mask.any():
            tp_hits[buy_mask] = (high_win[buy_mask] >= tp[buy_mask, None]).argmax(axis=1)
            sl_hits[buy_mask] = (low_win[buy_mask] <= sl[buy_mask, None]).argmax(axis=1)

        # Обработка продаж
        if sell_mask.any():
            tp_hits[sell_mask] = (low_win[sell_mask] <= tp[sell_mask, None]).argmax(axis=1)
            sl_hits[sell_mask] = (high_win[sell_mask] >= sl[sell_mask, None]).argmax(axis=1)

        return np.where(
            (tp_hits < sl_hits) |
            ((tp_hits != 0) & (sl_hits == 0)),
            True,
            False
        )

    # Применяем функцию только к валидным окнам
    profitable = np.full(len(df), False)
    results = calculate_profitability(
        df['takeprofit'].values[valid_indices],
        df['stoploss'].values[valid_indices],
        df['prev_direction'].values[valid_indices],
        high_windows,
        low_windows
    )

    # Заполняем результаты только для валидных индексов
    profitable[valid_indices] = results

    # 5. Записываем результат
    df['profitable'] = profitable
    df['profitable'] = df['profitable'].astype(int).replace({1: 1, 0: -1})
    # df.drop(['prev_direction', 'takeprofit', 'stoploss'], axis=1, inplace=True)
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
    df.to_csv("result_01.csv", index=False)

    profit_stats = df['profitable'].value_counts()
    print(profit_stats)
