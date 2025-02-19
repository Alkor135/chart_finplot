"""
Создание графика со всеми индикаторами и маркерами профита.
"""

import sys
import pandas as pd
import numpy as np
import finplot as fplt
from datetime import timezone

fplt.display_timezone = timezone.utc  # Настройка тайм зоны, чтобы не было смещения времени


def adaptive_laguerre_filter(df, alpha):
    """
    Добавляет колонку 'alf' в DataFrame с расчетом Adaptive Laguerre Filter (ALF).

    :param df: DataFrame с колонками ['datetime', 'open', 'close', 'high', 'low', 'volume']
    :param alpha: Коэффициент сглаживания (от 0 до 1)
    :return: DataFrame с добавленной колонкой 'alf'
    """
    # Проверка наличия необходимых колонок
    required_columns = {'datetime', 'open', 'close', 'high', 'low', 'volume'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"DataFrame должен содержать колонки: {required_columns}")

    # Создаем массив для хранения значений ALF
    n = len(df)
    alf_values = np.zeros(n)

    # Используем 'close' как входные данные для ALF
    prices = df['close'].values

    # Инициализация значений Laguerre
    L0 = L1 = L2 = L3 = 0

    for i in range(n):
        # Вывод процента обработки
        sys.stdout.write(f'\rProcessing ALF_{alpha}: {round(((i / n) * 100), 2)}%')
        sys.stdout.flush()

        price = prices[i]

        L0_old = L0
        L1_old = L1
        L2_old = L2

        L0 = alpha * price + (1 - alpha) * L0_old
        L1 = -(1 - alpha) * L0 + L0_old + (1 - alpha) * L1_old
        L2 = -(1 - alpha) * L1 + L1_old + (1 - alpha) * L2_old
        L3 = -(1 - alpha) * L2 + L2_old + (1 - alpha) * L3

        alf_values[i] = (L0 + 2 * L1 + 2 * L2 + L3) / 6

    # Добавляем колонку 'alf' в DataFrame
    df[f'alf_{alpha}'] = alf_values
    # Заполнение пропусков предыдущими значениями
    df[f'alf_{alpha}'] = df[f'alf_{alpha}'].fillna(method='ffill')
    return df


def volume_stops(df):
    # Инициализация новых колонок значением None
    df['long1'] = None
    df['short1'] = None
    df['long2'] = None
    df['short2'] = None

    # Проверка условий для каждой строки
    for i in range(2, len(df)):
        # Вывод процента обработки
        sys.stdout.write(f'\rProcessing VolumeStops: {round(((i / len(df)) * 100), 2)}%')
        sys.stdout.flush()

        if (df['volume'][i - 2] < df['volume'][i - 1] < df['volume'][i] and
                df['open'][i - 2] >= df['close'][i - 2] and df['open'][i - 1] >= df['close'][i - 1] and
                df['open'][i] <= df['close'][i]):
            df.at[i, 'long1'] = df['low'][i] - 40

        if (df['volume'][i - 2] < df['volume'][i - 1] < df['volume'][i] and
                df['open'][i - 2] <= df['close'][i - 2] and df['open'][i - 1] <= df['close'][i - 1] and
                df['open'][i] >= df['close'][i]):
            df.at[i, 'short1'] = df['high'][i] + 40

        if (df['volume'][i - 2] > df['volume'][i - 1] > df['volume'][i] and
                df['open'][i - 2] >= df['close'][i - 2] and df['open'][i - 1] >= df['close'][i - 1] and
                df['open'][i] <= df['close'][i]):
            df.at[i, 'long2'] = df['low'][i] - 40

        if (df['volume'][i - 2] > df['volume'][i - 1] > df['volume'][i] and
                df['open'][i - 2] <= df['close'][i - 2] and df['open'][i - 1] <= df['close'][i - 1] and
                df['open'][i] >= df['close'][i]):
            df.at[i, 'short2'] = df['high'][i] + 40

    return df


def chart_range(df, alpha_lst):
    df['datetime'] = pd.to_datetime(df['datetime'])  # Меняем тип данных в колонке

    # create two axes
    ax, ax2 = fplt.create_plot('RTS', rows=2)
    ax.set_visible(xgrid=True, ygrid=True)
    ax2.set_visible(xgrid=True, ygrid=True)

    # plot candle sticks
    # candles = df[['open', 'close', 'high', 'low']]  # Время в индексе
    candles = df[['datetime', 'open', 'close', 'high', 'low']]  # Время не в индексе
    fplt.candlestick_ochl(candles, ax=ax)

    # overlay volume on the top plot
    # volumes = df[['open','close','volume']]  # Время в индексе
    volumes = df[['datetime', 'open', 'close', 'volume']]  # Время не в индексе
    fplt.volume_ocv(volumes, ax=ax.overlay())

    for alpha in alpha_lst:
        fplt.plot(df[f'alf_{alpha}'], ax=ax, legend=f'ALF-{alpha}')

    # Volume Stops
    fplt.plot(df['long1'], legend='Long 1 Max volume', style='o', color='#00f')
    fplt.plot(df['long2'], legend='Long 2 Min Volume', style='o', color='#006400')
    fplt.plot(df['short1'], legend='Short 1 Max volume', style='o', color='#00f')
    fplt.plot(df['short2'], legend='Short 2 Min Volume', style='o', color='#006400')

    profitable = df[['datetime', 'open', 'close', 'profitable']]
    fplt.volume_ocv(profitable, colorfunc=fplt.strength_colorfilter, ax=ax2)
    fplt.add_legend('Прибыльность', ax=ax2)

    fplt.show()


if __name__ == '__main__':
    # Чтение данных
    df = pd.read_csv('result_01.csv')

    # Создаем список настроек индикаторов ALF
    alpha_lst = np.arange(0.30, 0.40, 0.01).tolist()
    alpha_lst = [round(alpha, 2) for alpha in alpha_lst]  # Округление значений списка

    # Фильтрация df
    # Регулярное выражение с незахватывающей группой (?:)
    pattern = r'(?:10:00:00|10:00:01|19:00:00|19:00:01|19:05:00|19:05:01)'
    # Фильтрация строк (удаляем строки с совпадением)
    df = df[~df['datetime'].str.contains(pattern, regex=True)]
    # Сброс индекса (переиндексация)
    df = df.reset_index(drop=True)

    # Добавление индикатора ALF
    for alpha in alpha_lst:
        df = adaptive_laguerre_filter(df, alpha=alpha)

    # Добавление индикатора VolumeStops
    df = volume_stops(df)

    # Настройки для отображения широкого df pandas
    pd.options.display.width = 1200
    pd.options.display.max_colwidth = 100
    pd.options.display.max_columns = 100
    print()
    print(df)

    # Создание графика
    chart_range(df, alpha_lst)
