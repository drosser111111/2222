# df_utils.py

# Импорты
import pandas as pd
from clickhouse_driver import Client
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from sqlalchemy import text
import pendulum
from tqdm import tqdm
import time
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import scipy.stats as st
import math as mth
import cmath
import datetime as dt
from datetime import timedelta
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import pymysql
import warnings
warnings.filterwarnings("ignore")
from tqdm.auto import tqdm
from statsmodels.stats.proportion import proportions_ztest, proportion_confint
import json
from operator import attrgetter
from IPython.display import HTML
import plotly.colors as pc
import scipy.stats as stats

import gc # Импортируем модуль сборщика мусора

print("df_utils.all_func() - вывод всех функций")
def helps():
 # Словарь с подсказками
 tips = {
  "data_sale_id.groupby('art_id')['user_id'].nunique().reset_index().sort_values(by = 'user_id' ,ascending=False).head(60)": "Группируем данные по 'art_id', подсчитываем уникальные 'user_id' в каждой группе, сортируем результаты по количеству уникальных 'user_id' в порядке убывания и выводим первые 60 записей. Это полезно для анализа взаимосвязей между артикулами и пользователями.",
  "df = df.rename(columns={'old_name1': 'new_name1', 'old_name2': 'new_name2'})": "Переименовываем столбцы 'old_name1' и 'old_name2' в 'new_name1' и 'new_name2' соответственно. Это полезно, когда нужно изменить имена столбцов для улучшения читаемости или соответствия стандартам.",
                                                                              " Импорт отсюда":" ", "df_utils.convert_columns_to_int_then_str([df_visit_book, df_vzat_group_by_day],  ['date', 'event_date', 'DATE_event_date', 'date_event_date', 'dt', 'sale_dt', 'payed', 'id', 'hub_user_id', 'user_id', 'hub_art_id', 'art_id', 'users', 'cnt_users', 'visiters', 'cnt_users_conv', 'cnt_users_visit'])": "функция обработки перед джоином",
  "функция обработки перед джоином": "df_utils.convert_columns_to_int_then_str([df_visit_book, df_vzat_group_by_day],  ['date', 'event_date', 'DATE_event_date', 'date_event_date', 'dt', 'sale_dt', 'payed', 'id', 'hub_user_id', 'user_id', 'hub_art_id', 'art_id', 'users', 'cnt_users', 'visiters', 'cnt_users_conv', 'cnt_users_visit'])",
  "Выполняем LEFT JOIN двух DataFrame по разным столбцам. Это позволяет объединить данные, сохраняя все строки из левого DataFrame и соответствующие строки из правого.": "result_vzat_visi = modul_import.pd.merge(df1, df2, left_on=['hub_art_id','sale_dt'], right_on=['hub_art_id','event_date'], how='left')",
  "подсчет непустых значений по столбцу который присоединился": "print(f\"Количество непустых значений в столбце 'cnt_users_conv': {result[result['cnt_users_conv'].notnull()]['cnt_users_conv'].count()}\")",                                                                                                                                                            "result[result['column'].notnull()]": "Показываем строки, где в столбце 'column' нет пустых значений. Это полезно для идентификации строк, где данные полностью заполнены.",
  "заполнение пустых значений": "result['cnt_users_conv'] = result['cnt_users_conv'].fillna('0')",
  "экспорт таблицы": "result.to_csv('df_new.csv', index=False)",
  "сброс юпитера": "%reset -f",
  "потом вернись вверх": "сбелай все импорты",
  "импорт таблицы": "df_new_df = modul_import.pd.read_csv('df_new.csv')",
                                                                             " ":" ",
  "summ_total_dwh = modul_import.pd.concat([filtered_summ_total_dwh_10_12,filtered_summ_total_dwh_7_10], ignore_index=True).drop_duplicates(subset=['sale_id'])": "Соединяем несколько DataFrame в один, игнорируя индексы, и удаляем дубликаты по указанному сабсету. Это полезно при объединении данных из разных источников.",
 "df[['column1', 'column2']]": "Выбираем определённые столбцы 'column1' и 'column2' из DataFrame. Это полезно, когда нужно работать только с определёнными данными.",
  "df['date_column'] = pd.to_datetime(df['date_column'])": "Приводим столбец 'date_column' к типу datetime. Это необходимо для работы с датами и временем.",
  "df[df['column'].isnull()]": "Показываем строки, где в столбце 'column' есть пустые значения. Это полезно для идентификации пропусков в данных.",
  "df['column'].fillna(value)": "Заполняем пустые значения в столбце 'column' указанным значением 'value'. Это полезно для обработки пропусков в данных.",
  "filtered_df = df[df.duplicated(subset=['column1', 'column2'], keep=False)]": "Находим строки с одинаковыми значениями в столбцах 'column1' и 'column2', но разными в другом столбце. Это полезно для выявления дубликатов по одному критерию, но с разными дополнительными атрибутами.",
  "filtered_df = df.loc[(df['column1'] == value1) & (df['column2'] == value2)]": "Выбираем строки, где 'column1' равно 'value1' И 'column2' равно 'value2'. Это полезно для фильтрации данных по нескольким критериям.",
  "filtered_df = df.loc[(df['column1'] == value1) | (df['column2'] == value2)]": "Выбираем строки, где 'column1' равно 'value1' ИЛИ 'column2' равно 'value2'. Это полезно для фильтрации данных по одному из нескольких критериев.",
  "df[column] = df[column].replace(pd.NaT, AsIs('NULL'))": "Заполнение строк даты пустых значений NULL для импорта в postgress."
 }

 # Выводим все подсказки с пробелами между ними
 for key, value in tips.items():
  print(f"{key}: {value}")
  print()  # Добавляем пустую строку для разделения подсказок




# функция жирного цветного текста
def print_colored_bold(text, color_code):
 # ANSI escape codes for colors and bold
 color_codes = {
  'black': 30,
  'red': 31,
  'green': 32,
  'yellow': 33,
  'blue': 34,
  'magenta': 35,
  'cyan': 36,
  'white': 37,
  'reset': 0  # Resets the color back to the terminal default
 }

 # Check if the color code is valid
 if color_code not in color_codes:
  print(f"Invalid color code: {color_code}")
  return

 # Print the text with the specified color and bold
 print(f"\033[1;{color_codes[color_code]}m{text}\033[0m")






# Определение функции analyze_dataframe которая выводит инф-ию о датафрейме и распределение значений столбца
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import gc

def analyze_dataframe(df, plot_column=None):
 # Вывод информации о типах данных
 print_colored_bold("Информация о типах данных:", 'red')
 df.info()

 # Проходим по всем колонкам DataFrame
 for column in df.columns:
  print(f"\nАнализ колонки: {column}")
  unique_values = df[column].unique()
  print(f"Уникальные значения (первые 10): {unique_values[:10]} (всего уникальных: {len(unique_values)})")
  empty_values_count = df[column].isnull().sum()
  empty_string_count = (df[column] == '').sum()
  print(f"Количество пустых значений: {empty_values_count}")
  print(f"Количество пустых строк (''): {empty_string_count}")

  if column in ['revenue', 'summ', 'amount', 'real_payed', 'payed', 'sum', 'payments', 'payment_amount', 'payment']:
   numeric_column = pd.to_numeric(df[column].replace('', pd.NA), errors='coerce')
   non_numeric_indices = numeric_column[numeric_column.isnull()].index
   non_numeric_values = df.loc[non_numeric_indices, column]
   if not non_numeric_values.empty:
    print(
     f"Значения, отличные от чисел в {column}: {non_numeric_values.unique()[:10]} (всего: {len(non_numeric_values.unique())})")
   else:
    print(f"Все значения в {column} являются числами или пустыми строками.")

 # Анализ и визуализация для указанной колонки
 if plot_column and plot_column in df.columns:
  print(f"\nАнализ и визуализация для колонки: {plot_column}")
  valid_data = pd.to_numeric(df[plot_column].replace('', pd.NA), errors='coerce').dropna()

  # Вычисляем медиану, среднее, моду и перцентили
  median = valid_data.median()
  mean = valid_data.mean()
  mode = valid_data.mode()[0]
  percentiles = valid_data.quantile([0.25, 0.5, 0.75, 0.95, 0.99]).to_dict()
  print(f"Медиана: {median}, Среднее: {mean}, Мода: {mode}")
  print_colored_bold("Перцентили:", 'red')
  for percentile, value in percentiles.items():
   print(f"{percentile * 100}-й перцентиль: {value}")

  # Фильтрация данных до 99-го перцентиля
  percentile_99 = valid_data.quantile(0.99)
  filtered_data = valid_data[valid_data < percentile_99]

  # Построение boxplot для всех данных
  plt.figure(figsize=(10, 6))
  sns.boxplot(x=valid_data)
  plt.title(f"Boxplot для {plot_column} (все данные)")
  plt.show()

  # Построение гистограммы распределения для всех данных
  plt.figure(figsize=(10, 6))
  sns.histplot(valid_data, kde=True)
  plt.title(f"Гистограмма распределения для {plot_column} (все данные)")
  plt.show()

  # Построение boxplot для данных до 99-го перцентиля
  plt.figure(figsize=(10, 6))
  sns.boxplot(x=filtered_data)
  plt.title(f"Boxplot для {plot_column} (< 99-й перцентиль)")
  plt.show()

  # Построение гистограммы распределения для данных до 99-го перцентиля
  plt.figure(figsize=(10, 6))
  sns.histplot(filtered_data, kde=True)
  plt.title(f"Гистограмма распределения для {plot_column} (< 99-й перцентиль)")
  plt.show()

  # Удаление объектов, чтобы освободить память
  del valid_data
  del filtered_data

 # Запускаем сборщик мусора для очистки памяти
 gc.collect()

# Пример использования функции
# analyze_dataframe(df, 'revenue')
# analyze_dataframe(df_success_purchase,'payment_amount')


import pandas as pd

import pandas as pd

import pandas as pd

import pandas as pd


def convert_columns_to_int_then_str(df_list, columns):
 """
 Функция для приведения указанных колонок DataFrame к типу int, а затем обратно к типу str.
 Если колонка содержит дату в формате 'YYYY-MM-DD', она сохраняется как строка.

 Параметры:
 df_list (list or pd.DataFrame): Список DataFrame или один DataFrame, в которых необходимо привести колонки к типу int, а затем обратно к типу str.
 columns (list): Список строк с названиями колонок, которые нужно привести к типу int, а затем обратно к типу str.

 Возвращает:
 list or pd.DataFrame: Список DataFrame или один DataFrame с приведенными к типу int, а затем обратно к типу str колонками.
 """
 # Если df_list - это один DataFrame, преобразуем его в список для единообразия обработки
 if isinstance(df_list, pd.DataFrame):
  df_list = [df_list]

 for df in df_list:
  for column in columns:
   # Проверяем, есть ли колонка в DataFrame
   if column in df.columns:
    # Преобразуем колонку к строковому типу, если это необходимо
    df[column] = df[column].astype(str)
    # Проверяем, содержит ли колонка даты в формате 'YYYY-MM-DD'
    if df[column].str.match(r'\d{4}-\d{2}-\d{2}').all():
     # Если да, то просто приводим колонку к типу str
     continue
    else:
     # Иначе приводим колонку к типу int, заменяем NaN на 0, а затем обратно к типу str
     df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int).astype(str)


 # Применяем функцию к списку DataFrame
 #df_list = convert_columns_to_int_then_str([df1, df2], columns)




def assign_host_group(df, columns_to_check):
    # Создаем новые столбцы host_group и host_mini_group
    df['host_big_group'] = ''
    df['host_mini_group'] = ''

    # Проверяем значения в столбцах и устанавливаем значение для host_big_group и host_mini_group
    for column in columns_to_check:
     # Устанавливаем значение для host_group
     df.loc[df[column].str.contains('ios_listen|ios_read|iOS Слушай|iOS Читай\+Слушай|iOS', case=False,
                                    na=False), 'host_big_group'] = 'ios'
     df.loc[df[column].str.contains('android_listen|android_read|Android Слушай|Android Читай\+Слушай|andorid|Andorid|Andrid|Android|android_read_free',
                                    case=False, na=False), 'host_big_group'] = 'android'
     df.loc[df[column].str.contains('PDA', case=False, na=False), 'host_big_group'] = 'pda'
     df.loc[df[column].str.contains('WEB', case=False, na=False), 'host_big_group'] = 'web'

     # Устанавливаем значение для host_mini_group
     df.loc[
      df[column].str.contains('ios_listen|iOS Слушай', case=False, na=False), 'host_mini_group'] = 'ios_listen'
     df.loc[df[column].str.contains('ios_read|iOS Читай\+Слушай', case=False, na=False), 'host_mini_group'] = 'ios_read'
     df.loc[df[column].str.contains('android_listen|Android Слушай', case=False,
                                    na=False), 'host_mini_group'] = 'android_listen'
     df.loc[df[column].str.contains('android_read|Android Читай\+Слушай|android_read_free', case=False, na=False), 'host_mini_group'] = 'android_read'
     df.loc[df[column].str.contains('PDA', case=False, na=False), 'host_mini_group'] = 'pda'
     df.loc[df[column].str.contains('WEB', case=False, na=False), 'host_mini_group'] = 'web'

    return df




import pandas as pd

def new_model_name(df, column):
    """
    Функция для замены "subs" или "Подписка" на "Подписка" в указанном столбце DataFrame.

    Параметры:
    df (pd.DataFrame): DataFrame, в котором нужно выполнить замену.
    column (str): Имя столбца, в котором нужно выполнить замену.

    Возвращает:
    pd.DataFrame: Измененный DataFrame.
    """
    # Создаем новый столбец model_new
    df['model_new'] = ''

    # Устанавливаем значение для model_new
    df.loc[df[column].str.contains('Подписка|subscr|subscription', case=False, na=False), 'model_new'] = 'subscription'
    df.loc[df[column].str.contains('abonement|Абонемент|abon', case=False, na=False), 'model_new'] = 'abonement'
    df.loc[df[column].str.contains('PPD|ppd', case=False, na=False), 'model_new'] = 'PPD'
    df.loc[~df[column].str.contains('PPD|ppd|Подписка|subscr|subscription|abonement|Абонемент|abon', case=False, na=False), 'model_new'] = 'PPD'

    # Преобразуем event_date в datetime, если это необходимо
    if 'event_date' in df.columns and df['event_date'].dtype != 'datetime64[ns]':
        df['event_date'] = pd.to_datetime(df['event_date'], format='%Y-%m-%d')

    # Проверяем, существует ли столбец event_type в DataFrame
    if 'event_type' in df.columns:
        # Добавляем логику для заполнения пустых значений в соответствии с новыми условиями
        comparison_date = pd.to_datetime('2024-03-10')
        df.loc[(df['event_type'] == 'payment_success') & (df['event_date'] < comparison_date), 'model_new'] = 'subscription'
        df.loc[(df['event_type'] == 'checkout_success') & (df['source_internal'] == 'subscription'), 'model_new'] = 'subscription'
        df.loc[(df['event_type'] == 'checkout_success') & (df['source_internal'] == 'abonement'), 'model_new'] = 'abonement'

    return df




def all_func():
    """
    Выводит примеры использования функций
    """
    # Пример использования функции helps()
    print("df_utils.helps() - вывод помощи при обработке")

    # Пример использования функции print_colored_bold()
    print(f'df_utils.print_colored_bold("Это жирный красный текст", "red") - эта функция выводит красный текст')

    # Пример использования функции analyze_dataframe()
    print(f'df_utils.analyze_dataframe(df_success_purchase,"payment_amount") - эта функция выводит информацио о датафрейме и значения для определенного столбца')

    # Пример использования функции convert_columns_to_int_then_str()
    print(f'df_utils.convert_columns_to_int_then_str([df1, df2], ["date","event_date","DATE_event_date","date_event_date","dt","sale_dt","payed", "id", "hub_user_id", "user_id", "hub_art_id", "art_id", "users", "cnt_users", "cnt_users_visit", "cnt_users_conv", "visiters"]) - эта функция сначала в инт потом в стринг, а колонки даты в стринг, перед джоинами выполнять')

    # Пример использования функции assign_host_group
    print("df_with_assign_host_group = df_utils.assign_host_group(df, ['host']) - добавление новых колонок с хостами")
    # df_with_assign_host_group = df_utils.assign_host_group(df, ['host'])

    # Пример использования функции model_name
    print("df_with_model = df_utils.new_model_name(df_with_assign_host_group, ['model_name']) - добавление новых колонок с моделью")
    # df_with_assign_host_group = df_utils.assign_host_group(df, ['host'])

