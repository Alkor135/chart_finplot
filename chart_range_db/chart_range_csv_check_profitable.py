"""
Создание графика со всеми индикаторами и маркерами профита.
"""

import pandas as pd
import numpy as np
import finplot as fplt
from datetime import timezone

fplt.display_timezone = timezone.utc  # Настройка тайм зоны, чтобы не было смещения времени


def chart_range(df):
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

    # TakeProfit
    fplt.plot(df['takeprofit'], legend='TakeProfit', style='o', color='#009900')
    # StopLoss
    fplt.plot(df['stoploss'], legend='StopLoss', style='o', color='#ff0000')

    profitable = df[['datetime', 'open', 'close', 'profitable']]
    fplt.volume_ocv(profitable, colorfunc=fplt.strength_colorfilter, ax=ax2)
    fplt.add_legend('Прибыльность', ax=ax2)

    fplt.show()


if __name__ == '__main__':
    # Чтение данных
    df = pd.read_csv('result.csv')

    # Фильтрация df
    # Регулярное выражение с незахватывающей группой (?:)
    pattern = r'(?:10:00:00|10:00:01|19:00:00|19:00:01|19:05:00|19:05:01)'
    # Фильтрация строк (удаляем строки с совпадением)
    df = df[~df['datetime'].str.contains(pattern, regex=True)]
    # Сброс индекса (переиндексация)
    df = df.reset_index(drop=True)

    # Настройки для отображения широкого df pandas
    pd.options.display.width = 1200
    pd.options.display.max_colwidth = 100
    pd.options.display.max_columns = 100
    print()
    print(df)

    # Создание графика
    chart_range(df)
