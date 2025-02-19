import pandas as pd
import numpy as np
import finplot as fplt
from datetime import timezone

fplt.display_timezone = timezone.utc  # Настройка тайм зоны, чтобы не было смещения времени


def adaptive_laguerre_filter(df, alpha=0.3):
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
    df['alf'] = alf_values
    return df


def volume_stops(df):
    # Инициализация новых колонок значением None
    df['long1'] = None
    df['short1'] = None
    df['long2'] = None
    df['short2'] = None

    # Проверка условий для каждой строки
    for i in range(2, len(df)):
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


if __name__ == '__main__':
    alpha=0.4

    df = pd.read_csv('cache.csv')
    df['datetime'] = pd.to_datetime(df['datetime'])
    # df = df.set_index('datetime')
    df = df.rename(columns={'vol':'volume'})
    df = adaptive_laguerre_filter(df, alpha=alpha)
    df = volume_stops(df)
    # Сохранение DF в файл без индекса
    df.to_csv(fr'alf_vs.csv', index=False)
    # print(df)
    # print(len(df))

    # create two axes
    ax = fplt.create_plot('RTS', rows=1)
    ax.set_visible(xgrid=True, ygrid=True)

    # plot candle sticks
    # candles = df[['open', 'close', 'high', 'low']]
    candles = df[['datetime', 'open', 'close', 'high', 'low']]
    fplt.candlestick_ochl(candles, ax=ax)

    # overlay volume on the top plot
    # volumes = df[['open','close','volume']]
    volumes = df[['datetime', 'open','close','volume']]
    fplt.volume_ocv(volumes, ax=ax.overlay())

    # put an ALF on the close price
    fplt.plot(df['datetime'], df['alf'], ax=ax, legend=f'ALF-{alpha}')

    # Volume Stops
    fplt.plot(df['datetime'], df['long1'], legend='Long 1 Max volume', style='o', color='#00f')
    # fplt.plot(df['datetime'], df['long2'], legend='Long 2 Min Volume', style='o', color='#f00')
    # fplt.plot(df['datetime'], df['long2'], legend='Long 2 Min Volume', style='o', color='#dc143c')
    fplt.plot(df['datetime'], df['long2'], legend='Long 2 Min Volume', style='o', color='#006400')
    fplt.plot(df['datetime'], df['short1'], legend='Short 1 Max volume', style='o', color='#00f')
    fplt.plot(df['datetime'], df['short2'], legend='Short 2 Min Volume', style='o', color='#006400')

    fplt.show()
