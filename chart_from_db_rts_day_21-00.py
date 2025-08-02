"""
Скрипт для чтения базы данных котировок фьючерсов RTS и отображения графика.
"""
import sqlite3
from pathlib import Path
import pandas as pd
import finplot as fplt


def main(path_db_day: Path) -> None:
    """
    Основная функция для чтения базы данных котировок и отображения графика.
    """
    # Подключение к базе данных SQLite
    conn = sqlite3.connect(path_db_day)

    # Чтение данных из таблицы в DataFrame
    df = pd.read_sql('SELECT * FROM Futures', conn)

    # Закрытие соединения
    conn.close()

    # Переименование колонок в нижний регистр
    df.columns = df.columns.str.lower()

    # Переименование колонки 'tradedate' в 'datetime'
    df = df.rename(columns={'tradedate': 'datetime'})

    # Преобразование столбца datetime в формат datetime64
    df['datetime'] = pd.to_datetime(df['datetime'])

    df = df.sort_values('datetime', ascending=True).reset_index()

    # Вывод последних 30 строк DataFrame
    print(df.tail(30).to_string(max_rows=30, max_cols=15))

    # Проверка наличия необходимых колонок
    required_columns = {'datetime', 'open', 'close', 'high', 'low'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"DataFrame должен содержать колонки: {required_columns}")

    # Проверка типов данных
    print(df.dtypes)  # Для отладки: убедитесь, что datetime имеет тип datetime64

    # Настройка цветов свечей
    fplt.candle_bull_body = '#0000FF'  # Синий для восходящих свечей (заливка тела)
    fplt.candle_bear_body = '#FF0000'  # Красный для нисходящих свечей (заливка тела)
    fplt.candle_bull_frame = '#0000FF'  # Синий для рамки восходящих свечей
    fplt.candle_bear_frame = '#FF0000'  # Красный для рамки нисходящих свечей

    # Создание графика с одной осью
    ax = fplt.create_plot('RTS', rows=1)
    ax.set_visible(xgrid=True, ygrid=True)

    # Подготовка данных для свечного графика
    candles = df[['datetime', 'open', 'close', 'high', 'low']]

    # Убедитесь, что числовые столбцы имеют правильный тип
    candles[['open', 'close', 'high', 'low']] = candles[['open', 'close', 'high', 'low']].astype(float)

    # Построение свечного графика
    fplt.candlestick_ochl(candles, ax=ax)

    # Отображение графика
    fplt.show()


if __name__ == '__main__':
    path_db_day = Path(r'C:\Users\Alkor\gd\data_quote_db\RTS_futures_day_2025_21-00.db')
    main(path_db_day)