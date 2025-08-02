"""
Система автоматических отчетов в Telegram

Этот файл содержит полную систему для автоматической генерации и отправки
аналитических отчетов в Telegram через Airflow. Система собирает данные из ClickHouse
и создает комплексные отчеты с графиками и метриками.

Автор: Алексей Полозов
Дата: 2025

ИСПОЛЬЗОВАНИЕ:
1. Автоматический запуск через Airflow (по расписанию)
2. Ручной запуск для тестирования (в конце файла)

ЗАВИСИМОСТИ:
pip install apache-airflow pandas numpy matplotlib seaborn pandahouse python-telegram-bot requests
"""

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import logging
import pandas as pd
import os

from datetime import datetime, timedelta
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def generate_basic_information(chat_id):
    bot = telegram.Bot(token=BOT_TOKEN)

    # Надпись первая строка отчет на какую дату
    MONTHS_RU = {'January': 'января', 'February': 'февраля', 'March': 'марта', 'April': 'апреля', 'May': 'мая', 'June': 'июня',
                 'July': 'июля', 'August': 'августа', 'September': 'сентября', 'October': 'октября', 'November': 'ноября', 'December': 'декабря'}
    yesterday = datetime.now() - timedelta(days=1)

    day = yesterday.strftime('%d')

    month_en = yesterday.strftime('%B')
    month_ru = MONTHS_RU[month_en]

    year = yesterday.strftime('%Y')

    # СОБИРАЕМ МЕТРИКИ
    # МЕТРИКА 1
    # метрика количество уникальных пользователей
    total_users = '''WITH total_users AS (
                                        SELECT DISTINCT user_id FROM simulator_20250620.feed_actions WHERE toDate(time) < today()
                                        UNION ALL
                                        SELECT DISTINCT user_id FROM simulator_20250620.message_actions WHERE toDate(time) < today()
                                            )
                         SELECT count(user_id) AS users
                         FROM total_users'''

    df_users = ph.read_clickhouse(query=total_users, connection=connection)
    users = df_users['users'].iloc[0]

    # МЕТРИКА 2
    # Метрика доля платных и органических пользователей
    doly_organic_ads = '''SELECT DISTINCT user_id, source FROM simulator_20250620.feed_actions WHERE toDate(time) < today() 
                          UNION ALL
                          SELECT DISTINCT user_id, source FROM simulator_20250620.message_actions WHERE toDate(time) < today()'''

    df_doly_organic_ads = ph.read_clickhouse(
        query=doly_organic_ads, connection=connection)

    # Подсчитываем количество пользователей по источникам
    source_counts = df_doly_organic_ads.groupby(
        'source')['user_id'].nunique().reset_index()
    source_counts.columns = ['source', 'user_count']

    # Вычисляем доли
    total_users = source_counts['user_count'].sum()
    source_counts['percentage'] = (
        source_counts['user_count'] / total_users * 100).round(2)

    # Записываем в переменные значения из датафрейма
    users_ads = source_counts['percentage'].iloc[0]
    users_organic = source_counts['percentage'].iloc[1]

    # МЕТРИКА 3
    # Метрика Среднее (медиана) количество лайков и просмотров на 1 пользователя
    average_user_like_view = '''SELECT user_id AS user,
                                   source,
                                sum(action = 'like') AS likes,
                                sum(action = 'view') AS views
                            FROM simulator_20250620.feed_actions
                            WHERE toDate(time) < today()
                            GROUP BY user_id, source'''

    df_average_user_like_view = ph.read_clickhouse(
        query=average_user_like_view, connection=connection)

    median_like_ads = df_average_user_like_view[df_average_user_like_view['source'] == 'ads']['likes'].median(
    )
    median_like_ads = int(median_like_ads)

    median_view_ads = df_average_user_like_view[df_average_user_like_view['source'] == 'ads']['views'].median(
    )
    median_view_ads = int(median_view_ads)

    median_like_organic = df_average_user_like_view[df_average_user_like_view['source'] == 'organic']['likes'].median(
    )
    median_like_organic = int(median_like_organic)

    median_view_organic = df_average_user_like_view[df_average_user_like_view['source'] == 'organic']['views'].median(
    )
    median_view_organic = int(median_view_organic)

    # МЕТРИКА 4
    # Метрика Среднее (медиана) количество отправленых сообщений на 1 пользователя
    average_sent_message_view = '''SELECT 
                                        user_id AS user,
                                        source,
                                        count(*) AS sent_messages
                                  FROM simulator_20250620.message_actions
                                  WHERE toDate(time) < today()
                                  GROUP BY user_id, source'''

    df_average_sent_message_view = ph.read_clickhouse(
        query=average_sent_message_view, connection=connection)

    median_message = df_average_sent_message_view.groupby(
        'source')['sent_messages'].median().astype(int).reset_index()
    median_message_ads = median_message.iloc[0, 1]
    median_message_ogranic = median_message.iloc[1, 1]

    message = f'Отчет на {day} {month_ru} {year}\n'
    message += f'Общие метрики (за весь период):\n'
    message += f'- Количество уникальных пользователей: {users}\n'
    message += f'- Доля рекламных пользователей:  {users_ads}%\n'
    message += f'- Доля органических пользователей:  {users_organic}%\n'

    message += f'\n'

    message += f'Лайки на пользователя (медиана):\n'
    message += f'- Платный трафик:  {median_like_ads}\n'
    message += f'- Органический трафик:  {median_like_organic}\n'

    message += f'\n'

    message += f'Просмотры на пользователя (медиана):\n'
    message += f'- Платный трафик:  {median_view_ads}\n'
    message += f'- Органический трафик:  {median_view_organic}\n'

    message += f'\n'

    message += f'Сообщения на пользователя (медиана):\n'
    message += f'- Платный трафик:  {median_message_ads}\n'
    message += f'- Органический трафик:  {median_message_ogranic}\n'

    bot.sendMessage(chat_id=chat_id, text=message)


def send_plot(df_dau_source, df_like_views_source, df_sent_message, bot, chat_id):

    # Создаём фигуру с компоновкой: 1 график сверху, 2x2 снизу
    fig = plt.figure(figsize=(16, 14))

    # Создаём сетку: 3 ряда, 2 колонки
    # Первый график занимает всю ширину (span=2)
    ax1 = plt.subplot(3, 2, (1, 2))  # Первый график на всю ширину

    # Остальные 4 графика в сетке 2x2
    ax2 = plt.subplot(3, 2, 3)  # Второй график
    ax3 = plt.subplot(3, 2, 4)  # Третий график
    ax4 = plt.subplot(3, 2, 5)  # Четвёртый график
    ax5 = plt.subplot(3, 2, 6)  # Пятый график

    # График 1: DAU по источникам (большой, сверху)
    sns.lineplot(data=df_dau_source, x='date', y='dau', hue='source',
                 marker='o', ax=ax1)
    ax1.set_title('Количество уникальных пользователей на обеих платформах',
                  fontsize=14, fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Количество пользователей')
    ax1.tick_params(axis='x', rotation=0)

    # График 2: Лайки по источникам
    sns.lineplot(data=df_like_views_source, x='date', y='likes', hue='source',
                 marker='s', ax=ax2)
    ax2.set_title('Количество лайков в ленте новостей',
                  fontsize=12, fontweight='bold')
    ax2.set_xlabel('Дата')
    ax2.set_ylabel('Количество лайков')
    ax2.tick_params(axis='x', rotation=0)

    # График 3: Просмотры по источникам
    sns.lineplot(data=df_like_views_source, x='date', y='views', hue='source',
                 marker='^', ax=ax3)
    ax3.set_title('Количество просмотров в ленте новостей',
                  fontsize=12, fontweight='bold')
    ax3.set_xlabel('Дата')
    ax3.set_ylabel('Количество просмотров')
    ax3.tick_params(axis='x', rotation=0)

    # График 4: Количество отправленных сообщений по источникам
    sns.lineplot(data=df_sent_message, x='date', y='sent_messages', hue='source',
                 marker='d', ax=ax4)
    ax4.set_title('Количество отправленных сообщений в мессенджере',
                  fontsize=12, fontweight='bold')
    ax4.set_xlabel('Дата')
    ax4.set_ylabel('Количество отправленых сообщений')
    ax4.tick_params(axis='x', rotation=0)

    # График 5: Количество пользователей по источникам
    sns.lineplot(data=df_sent_message, x='date', y='unique_senders', hue='source',
                 marker='*', ax=ax5)
    ax5.set_title('Количество пользователей отправивших сообщения в мессенджере',
                  fontsize=12, fontweight='bold')
    ax5.set_xlabel('Дата')
    ax5.set_ylabel('Количество пользователей')
    ax5.tick_params(axis='x', rotation=0)

    # Общий заголовок
    fig.suptitle('Графики метрик', fontsize=16, fontweight='bold', y=0.98)

    # Настраиваем отступы
    plt.tight_layout()

    # Сохраняем и отправляем
    plot_object = io.BytesIO()
    plt.savefig(plot_object, dpi=300, bbox_inches='tight')
    plot_object.seek(0)
    plot_object.name = 'full_report_5_graphs.png'
    plt.close()

    bot.sendMessage(chat_id=chat_id, text="📊 Графики метрик")
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


def send_plot_audience(df_action_audience, bot, chat_id):
    plt.figure(figsize=(14, 7))

    sns.barplot(data=df_action_audience, x='this_week',
                y='users_count', hue='status')

    plt.title('Динамика пользователей по статусам (ушедшие, старые, новые)',
              fontsize=14, fontweight='bold')
    plt.xlabel('Неделя')
    plt.ylabel('Количество пользователей')
    plt.xticks(rotation=45)

    # Сохраняем и отправляем
    plot_object_1 = io.BytesIO()
    plt.savefig(plot_object_1, dpi=300, bbox_inches='tight')
    plot_object_1.seek(0)
    plot_object_1.name = 'full_report_audience.png'
    plt.close()

    bot.sendMessage(chat_id=chat_id,
                    text="📊 График активная аудитория по неделям")
    bot.sendPhoto(chat_id=chat_id, photo=plot_object_1)


def generate_report_plot(chat_id):
    bot = telegram.Bot(token=BOT_TOKEN)

    # График 1 - DAU с разделенеим трафика на платных и органику
    graphics_DAU_source = '''SELECT toDate(time) AS date, 
                               source,
                               count(DISTINCT user_id) AS dau
                        FROM (
                            SELECT user_id, time, source FROM simulator_20250620.feed_actions WHERE toDate(time) < today()
                            UNION ALL
                            SELECT user_id, time, source FROM simulator_20250620.message_actions WHERE toDate(time) < today()
                        ) combined_actions
                        GROUP BY date, source
                        ORDER BY date, source'''

    df_dau_source = ph.read_clickhouse(
        query=graphics_DAU_source, connection=connection)

    # График 2 - Лайки и просмотры с разделенеим трафика на платных и органику
    graphics_like_views_source = '''SELECT toDate(time) AS date, 
                                        source,
                                        sum(action = 'like') AS likes,
                                        sum(action = 'view') AS views
                                  FROM simulator_20250620.feed_actions
                                  WHERE toDate(time) < today()
                                  GROUP BY date, source
                                  ORDER BY date, source'''

    df_like_views_source = ph.read_clickhouse(
        query=graphics_like_views_source, connection=connection)

    # График 3 - Отправление сообщения с разделенеим трафика на платных и органику
    graphics_sent_message = '''SELECT toDate(time) AS date, 
                                      source,
                                      count(*) AS sent_messages,
                                      count(DISTINCT user_id) AS unique_senders
                               FROM simulator_20250620.message_actions
                               WHERE toDate(time) < today()
                               GROUP BY date, source
                               ORDER BY date, source'''

    df_sent_message = ph.read_clickhouse(
        query=graphics_sent_message, connection=connection)

    # График 4 - Старые, новые, ушедшие пользователи по неделям
    graphics_action_audience = '''WITH weeks_data AS (
                                                    SELECT DISTINCT user_id,
                                                           toMonday(time)::date AS week
                                                    FROM simulator_20250620.feed_actions
                                                    WHERE toMonday(time)::date < toMonday(today())  -- исключаем текущую неделю
                                                    ),

                                       user_weeks_visited AS (
                                                    SELECT
                                                            user_id,
                                                            groupArray(week) AS weeks_visited
                                                    FROM weeks_data
                                                    GROUP BY user_id
                                                    )

                                                    -- ушедшие
                                    SELECT
                                          addWeeks(w1.week, 1) AS this_week,
                                          w1.week AS previous_week,
                                          'ушедшие' AS status,
                                           (count(DISTINCT uwv.user_id) * -1)::Int64 AS users_count
                                    FROM user_weeks_visited uwv
                                    JOIN weeks_data w1 ON uwv.user_id = w1.user_id
                                    WHERE has(uwv.weeks_visited, addWeeks(w1.week, 1)) = 0
                                    GROUP BY this_week, previous_week, status

                                    UNION ALL

                                                      -- старые и новые
                                    SELECT
                                          w1.week AS this_week,
                                          addWeeks(w1.week, -1) AS previous_week,
                                          IF(has(uwv.weeks_visited, addWeeks(w1.week, -1)), 'старые', 'новые') AS status,
                                          count(DISTINCT uwv.user_id)::Int64 AS users_count
                                    FROM user_weeks_visited uwv
                                    JOIN weeks_data w1 ON uwv.user_id = w1.user_id
                                    GROUP BY this_week, previous_week, status
                                    ORDER BY this_week, status'''

    df_action_audience = ph.read_clickhouse(
        query=graphics_action_audience, connection=connection)
    # df_action_audience['this_week'] = pd.to_datetime(df_action_audience['this_week'])
    # df_action_audience['previous_week'] = pd.to_datetime(df_action_audience['previous_week'])
    df_action_audience = df_action_audience.sort_values(
        ['this_week', 'status'])  # сортируем по недели и статусу

    # Проверяем данные  # ТУТ ЕСЛИ ПРИДЕТ ПУСТОЙ ДАТАФРЕМ ТО У НАС НЕ СЛОМАЕТСЯ ДАГ А ПРИДЕТ ПРОСТО СООБЩЕНИЕ ЧТО НЕТ ДАННЫХ
    if df_dau_source.empty or df_like_views_source.empty or df_sent_message.empty:
        bot.sendMessage(chat_id=chat_id, text="Нет данных для отчёта.")
        return

    # Конвертируем даты
    df_dau_source['date'] = pd.to_datetime(df_dau_source['date'])
    df_like_views_source['date'] = pd.to_datetime(df_like_views_source['date'])
    df_sent_message['date'] = pd.to_datetime(df_sent_message['date'])
    df_action_audience['this_week'] = pd.to_datetime(
        df_action_audience['this_week']).dt.strftime('%Y-%m-%d')

    # Вызов функции где будут строится графики передаем 3 датафрейма, номер чата в телеграме, имя бота
    send_plot(df_dau_source, df_like_views_source,
              df_sent_message, bot, chat_id)

    send_plot_audience(df_action_audience, bot, chat_id)


def send_plot_lenta(df_block_lenta, bot, chat_id):

    # Создаём фигуру с компоновкой: 1 график сверху, 2x2 снизу
    fig = plt.figure(figsize=(16, 14))

    # Создаём сетку: 2 ряда, 2 колонки
    ax1 = plt.subplot(2, 2, 1)  # Первый график
    ax2 = plt.subplot(2, 2, 2)  # Второй график
    ax3 = plt.subplot(2, 2, 3)  # Третий график
    ax4 = plt.subplot(2, 2, 4)  # Четвёртый график

    # График 1: DAU
    sns.barplot(data=df_block_lenta, x='event_date',
                y='dau', ax=ax1, color='#4c72b0')
    ax1.set_title('Количество уникальных пользователей',
                  fontsize=14, fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Количество пользователей')
    ax1.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax1.set_ylim(0, df_block_lenta['dau'].max() * 1.8)

    # График 2: Лайки
    sns.lineplot(data=df_block_lenta, x='event_date',
                 y='likes', ax=ax2, color='#4c72b0')
    ax2.set_title('Количество лайков', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Дата')
    ax2.set_ylabel('Количество лайков')
    ax2.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax2.set_ylim(0, df_block_lenta['likes'].max() * 1.2)

    # График 3: Просмотры по источникам
    sns.lineplot(data=df_block_lenta, x='event_date',
                 y='views', ax=ax3, color='#4c72b0')
    ax3.set_title('Количество просмотров', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Дата')
    ax3.set_ylabel('Количество просмотров')
    ax3.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax3.set_ylim(0, df_block_lenta['views'].max() * 1.2)

    # График 4: CTR
    sns.barplot(data=df_block_lenta, x='event_date',
                y='CTR', ax=ax4, color='#4c72b0')
    ax4.set_title('CTR', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Дата')
    ax4.set_ylabel('CTR')
    ax4.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax4.set_ylim(0, df_block_lenta['CTR'].max() * 1.8)

    # Общий заголовок
    fig.suptitle('Графики метрик в ленте новостей за предыдущую неделю',
                 fontsize=16, fontweight='bold', y=1)

    # Настраиваем отступы
    plt.tight_layout()

    # Сохраняем и отправляем
    plot_object = io.BytesIO()
    plt.savefig(plot_object, dpi=300, bbox_inches='tight')
    plot_object.seek(0)
    plot_object.name = 'lenta_report_graphs.png'
    plt.close()

    diapazon = f"c {df_block_lenta['event_date'].min()} по {df_block_lenta['event_date'].max()}"

    bot.sendMessage(chat_id=chat_id,
                    text=f"📊 Графики метрик в ленте новостей {diapazon}")
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


def generate_lenta_information(chat_id):
    bot = telegram.Bot(token=BOT_TOKEN)

    # Надпись первая строка отчет на какую дату
    MONTHS_RU = {'January': 'января', 'February': 'февраля', 'March': 'марта', 'April': 'апреля', 'May': 'мая', 'June': 'июня',
                 'July': 'июля', 'August': 'августа', 'September': 'сентября', 'October': 'октября', 'November': 'ноября', 'December': 'декабря'}
    yesterday = datetime.now() - timedelta(days=1)

    day = yesterday.strftime('%d')

    month_en = yesterday.strftime('%B')
    month_ru = MONTHS_RU[month_en]

    year = yesterday.strftime('%Y')

    # СОБИРАЕМ МЕТРИКИ по ленте за вчера и неделю назад
    # МЕТРИКА 1 - 4
    # метрика DAU, like, view, CTR
    block_lenta = '''SELECT 
                        toDate(time) AS event_date,
                        count(DISTINCT user_id) AS dau,
                        sum(action = 'view') AS views,
                        sum(action = 'like') AS likes,
                        sum(action = 'like') / sum(action = 'view') AS CTR
                    FROM simulator_20250620.feed_actions
                    WHERE toDate(time) BETWEEN today() - 8 AND yesterday()
                    GROUP BY event_date
                    ORDER BY event_date;'''

    df_block_lenta = ph.read_clickhouse(
        query=block_lenta, connection=connection)

    # Проверяем данные  # ТУТ ЕСЛИ ПРИДЕТ ПУСТОЙ ДАТАФРЕМ ТО У НАС НЕ СЛОМАЕТСЯ ДАГ А ПРИДЕТ ПРОСТО СООБЩЕНИЕ ЧТО НЕТ ДАННЫХ
    if df_block_lenta.empty:
        bot.sendMessage(chat_id=chat_id, text="Нет данных для отчёта.")
        return

    # Конвертируем даты
    df_block_lenta['event_date'] = df_block_lenta['event_date'].dt.strftime(
        '%Y-%m-%d')

    # МЕТРИКА DAU
    yesterday_dau = df_block_lenta['dau'].iloc[-1]
    week_dau = df_block_lenta['dau'].iloc[:-1].mean().astype(int)

    delta_dau = (((yesterday_dau - week_dau) / week_dau) * 100).round(2)

    # МЕТРИКА like
    yesterday_like = df_block_lenta['likes'].iloc[-1]
    week_like = df_block_lenta['likes'].iloc[:-1].mean().astype(int)

    delta_like = (((yesterday_like - week_like) / week_like) * 100).round(2)

    # МЕТРИКА view
    yesterday_view = df_block_lenta['views'].iloc[-1]
    week_view = df_block_lenta['views'].iloc[:-1].mean().astype(int)

    delta_view = (((yesterday_view - week_view) / week_view) * 100).round(2)

    # МЕТРИКА CTR
    yesterday_CTR = ((df_block_lenta['CTR'].iloc[-1]) * 100).round(2)
    week_CTR = ((df_block_lenta['CTR'].iloc[:-1].mean()) * 100).round(2)

    delta_CTR = ((yesterday_CTR - week_CTR) / week_CTR).round(3)

    def format_change(value, is_pp=False):
        sign = '+' if value > 0 else ('−' if value < 0 else '')
        value = abs(value)
        if is_pp:
            return f"{sign}{value:.3f} п.п."
        else:
            return f"{sign}{value:.0f}%"

    message = f'Метрики ленты новостей за {day} {month_ru} {year}\n'
    message += f'(в скобках — изменение вчерашних значений по сравнению со средним значением за предыдущие 7 дней):\n'
    message += f'- DAU: {yesterday_dau} ({format_change(delta_dau)})\n'
    message += f'- Количество просмотров: {yesterday_view} ({format_change(delta_view)})\n'
    message += f'- Количество лайков: {yesterday_like} ({format_change(delta_like)})\n'
    message += f'- CTR: {yesterday_CTR}% ({format_change(delta_CTR, is_pp=True)})\n'

    bot.sendMessage(chat_id=chat_id, text=message)

    # Вызов функции где будут строится графики передаем
    send_plot_lenta(df_block_lenta, bot, chat_id)


def send_plot_message(df_block_message, bot, chat_id):

    # Создаём фигуру с компоновкой: 1 график сверху, 2x2 снизу
    fig = plt.figure(figsize=(16, 14))

    # Создаём сетку: 2 ряда, 2 колонки
    ax1 = plt.subplot(2, 2, 1)  # Первый график
    ax2 = plt.subplot(2, 2, 2)  # Второй график
    ax3 = plt.subplot(2, 2, 3)  # Третий график
    ax4 = plt.subplot(2, 2, 4)  # Четвёртый график

    # График 1: DAU
    sns.barplot(data=df_block_message, x='event_date',
                y='dau', ax=ax1, color='#4c72b0')
    ax1.set_title('Количество уникальных пользователей',
                  fontsize=14, fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Количество пользователей')
    ax1.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax1.set_ylim(0, df_block_message['dau'].max() * 1.2)

    # График 2: Отправленные сообщения
    sns.lineplot(data=df_block_message, x='event_date',
                 y='messages_sent', ax=ax2, color='#4c72b0')
    ax2.set_title('Количество отправленных сообщений',
                  fontsize=14, fontweight='bold')
    ax2.set_xlabel('Дата')
    ax2.set_ylabel('Количество отправленных сообщений')
    ax2.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax2.set_ylim(0, df_block_message['messages_sent'].max() * 1.2)

    # нормализация оси Y
    # import matplotlib.ticker as mtick
    # ax2.yaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.0f}'))

    # График 3: Среднее на пользователя
    sns.lineplot(data=df_block_message, x='event_date',
                 y='avg_per_user', ax=ax3, color='#4c72b0')
    ax3.set_title('Среднее на одного пользователя',
                  fontsize=14, fontweight='bold')
    ax3.set_xlabel('Дата')
    ax3.set_ylabel('Количество отправленых сообщений')
    ax3.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax3.set_ylim(0, df_block_message['avg_per_user'].max() * 1.2)

    # График 4: Медиана на пользователя
    sns.lineplot(data=df_block_message, x='event_date',
                 y='median_per_user', ax=ax4, color='#4c72b0')
    ax4.set_title('Медиана на одного пользователя',
                  fontsize=14, fontweight='bold')
    ax4.set_xlabel('Дата')
    ax4.set_ylabel('Количество отправленых сообщений')
    ax4.tick_params(axis='x', rotation=45)
    # Добавь отступ по оси Y:
    ax4.set_ylim(0, df_block_message['median_per_user'].max() * 1.2)

    # Общий заголовок
    fig.suptitle('Графики метрик предыдущую неделю',
                 fontsize=16, fontweight='bold', y=1)

    # Настраиваем отступы
    plt.tight_layout()

    # Сохраняем и отправляем
    plot_object = io.BytesIO()
    plt.savefig(plot_object, dpi=300, bbox_inches='tight')
    plot_object.seek(0)
    plot_object.name = 'message_report_graphs.png'
    plt.close()

    diapazon = f"c {df_block_message['event_date'].min()} по {df_block_message['event_date'].max()}"

    bot.sendMessage(chat_id=chat_id,
                    text=f"📊 Графики по метрикам в мессенджере {diapazon}")
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


def generate_message_information(chat_id):
    bot = telegram.Bot(token=BOT_TOKEN)

    # Надпись первая строка отчет на какую дату
    MONTHS_RU = {'January': 'января', 'February': 'февраля', 'March': 'марта', 'April': 'апреля', 'May': 'мая', 'June': 'июня',
                 'July': 'июля', 'August': 'августа', 'September': 'сентября', 'October': 'октября', 'November': 'ноября', 'December': 'декабря'}
    yesterday = datetime.now() - timedelta(days=1)

    day = yesterday.strftime('%d')

    month_en = yesterday.strftime('%B')
    month_ru = MONTHS_RU[month_en]

    year = yesterday.strftime('%Y')

    # СОБИРАЕМ МЕТРИКИ по сообщениям за вчера и неделю назад
    # МЕТРИКА 1 - 4
    # метрика DAU, messages_sent, median_per_user, avg_per_user
    message_information = '''-- Подзапрос 1: считаем общие метрики
                        WITH main_stats AS (
                                            SELECT 
                                                    toDate(time) AS event_date,
                                                    count(*) AS messages_sent,
                                                    count(DISTINCT user_id) AS dau,
                                                    round(count(*) / count(DISTINCT user_id), 2) AS avg_per_user
                                            FROM simulator_20250620.message_actions
                                            WHERE toDate(time) BETWEEN today() - 8 AND yesterday()
                                            GROUP BY event_date
                                                    ),

                    -- Подзапрос 2: считаем медиану по пользователям за каждый день
                                medians AS (
                                            SELECT 
                                                    event_date,
                                                    quantile(0.5)(sent_messages) AS median_per_user
                                            FROM (
                                                    SELECT 
                                                        toDate(time) AS event_date,
                                                        user_id,
                                                        count(*) AS sent_messages
                                                    FROM simulator_20250620.message_actions
                                                    WHERE toDate(time) BETWEEN today() - 8 AND yesterday()
                                                    GROUP BY event_date, user_id
                                                    )
                                            GROUP BY event_date
                                            )

                    -- Объединяем
                        SELECT 
                            m.event_date,
                            m.messages_sent,
                            m.dau,
                            m.avg_per_user,
                            med.median_per_user
                        FROM main_stats AS m
                        JOIN medians AS med ON m.event_date = med.event_date
                        ORDER BY m.event_date;'''

    df_block_message = ph.read_clickhouse(
        query=message_information, connection=connection)

    # Проверяем данные  # ТУТ ЕСЛИ ПРИДЕТ ПУСТОЙ ДАТАФРЕМ ТО У НАС НЕ СЛОМАЕТСЯ ДАГ А ПРИДЕТ ПРОСТО СООБЩЕНИЕ ЧТО НЕТ ДАННЫХ
    if df_block_message.empty:
        bot.sendMessage(chat_id=chat_id, text="Нет данных для отчёта.")
        return

    # Конвертируем даты
    df_block_message['event_date'] = df_block_message['event_date'].dt.strftime(
        '%Y-%m-%d')

    # МЕТРИКА DAU
    yesterday_dau = df_block_message['dau'].iloc[-1]
    week_dau = df_block_message['dau'].iloc[:-1].mean().astype(int)

    delta_dau = (((yesterday_dau - week_dau) / week_dau) * 100).round(2)

    # МЕТРИКА messages_sent
    yesterday_messages_sent = df_block_message['messages_sent'].iloc[-1]
    week_messages_sent = df_block_message['messages_sent'].iloc[:-
                                                                1].mean().astype(int)

    delta_messages_sent = (
        ((yesterday_messages_sent - week_messages_sent) / week_messages_sent) * 100).round(2)

    # МЕТРИКА median_per_user
    yesterday_median_per_user = df_block_message['median_per_user'].iloc[-1]
    week_median_per_user = df_block_message['median_per_user'].iloc[:-1].mean(
    ).astype(int)

    delta_median_per_user = (
        ((yesterday_median_per_user - week_median_per_user) / week_median_per_user) * 100).round(2)

    # МЕТРИКА avg_per_user
    yesterday_avg_per_user = df_block_message['avg_per_user'].iloc[-1]
    week_avg_per_user = df_block_message['avg_per_user'].iloc[:-
                                                              1].mean().astype(int)

    delta_avg_per_user = (
        ((yesterday_avg_per_user - week_avg_per_user) / week_avg_per_user) * 100).round(2)

    def format_change(value, is_pp=False):
        sign = '+' if value > 0 else ('−' if value < 0 else '')
        value = abs(value)
        return f"{sign}{value:.0f}%"

    message = f'Метрики в мессенджере за {day} {month_ru} {year}\n'
    message += f'(в скобках — изменение вчерашних значений по сравнению со средним значением за предыдущие 7 дней):\n'
    message += f'- DAU: {yesterday_dau} ({format_change(delta_dau)})\n'
    message += f'- Количество отправленых сообщений: {yesterday_messages_sent} ({format_change(delta_messages_sent)})\n'
    message += f'- Медиана: {yesterday_median_per_user} ({format_change(delta_median_per_user)})\n'
    message += f'- Среднее: {yesterday_avg_per_user} ({format_change(delta_avg_per_user)})\n'

    bot.sendMessage(chat_id=chat_id, text=message)

    # Вызов функции где будут строится графики передаем
    send_plot_message(df_block_message, bot, chat_id)


connection = {
    'host': 'Ваши данные к подключению к Clickhouse',
    'database': 'Ваши данные',
    'user': 'Ваши данные',
    'password': 'Ваши данные'
}

# === CONFIG ===
BOT_TOKEN = 'Ваш токен'
chat_id = 'Ваш ID чата'

default_args = {
    'owner': 'aleksej-polozov-bel8894',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'


@dag(dag_id='aleksej_polozov_bel8894_full_report', default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report():

    @task()
    def report_text_task():
        generate_basic_information(chat_id)

    @task()
    def report_plot_task():
        generate_report_plot(chat_id)

    @task()
    def report_text_lenta_task():
        generate_lenta_information(chat_id)

    @task()
    def report_text_message_task():
        generate_message_information(chat_id)

    # Вызываем таски — создаём зависимости
    task1 = report_text_task()
    task2 = report_plot_task()
    task3 = report_text_lenta_task()
    task4 = report_text_message_task()

    # порядок выполнения
    task1 >> task2 >> task3 >> task4


dag = dag_report()

# ============================================================================
# РУЧНОЙ ЗАПУСК (для тестирования)
# ============================================================================

if __name__ == "__main__":
    """
    Ручной запуск системы для тестирования.
    Запустите: python telegram_reports_system.py
    """
    print("🚀 Запуск системы автоматических отчетов...")

    try:
        # 1. Генерация базового отчета
        print("📊 Генерация базового отчета...")
        generate_basic_information(chat_id)

        # 2. Создание графиков
        print("📈 Создание графиков...")
        generate_report_plot(chat_id)

        # 3. Отчет по ленте новостей
        print("📰 Отчет по ленте новостей...")
        generate_lenta_information(chat_id)

        # 4. Отчет по мессенджеру
        print("💬 Отчет по мессенджеру...")
        generate_message_information(chat_id)

        print("✅ Все отчеты успешно отправлены!")

    except Exception as e:
        print(f"❌ Ошибка при выполнении: {e}")
        print("💡 Убедитесь, что:")
        print("   - Установлены все зависимости: pip install apache-airflow pandas numpy matplotlib seaborn pandahouse python-telegram-bot requests")
        print("   - Настроены переменные окружения (BOT_TOKEN, CHAT_ID, CLICKHOUSE_*)")
        print("   - Бот добавлен в чат и имеет права на отправку сообщений")
