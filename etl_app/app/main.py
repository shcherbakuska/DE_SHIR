import requests
import pandas as pd
import numpy as np
import sys
import json
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from database import SessionLocal, WeatherRecords

# Функция извлечения данных в формате json по заданному API
def extract(start_date: str, end_date: str) -> dict:
    URI=f"https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date={start_date}&end_date={end_date}"
    response = requests.get(URI)
    data = response.json()
    return data

# Функция трансформации данных для получения итоговой таблицы
def transform(data: dict) -> pd.DataFrame:
    df_hourly=pd.DataFrame(data['hourly'])
    df_daily=pd.DataFrame(data['daily'])
    df_hourly['time']=pd.to_datetime(df_hourly['time'], unit='s')
    # Переводим Фаренгейты в градусы Цельсия
    temperature=['temperature_2m', 'dew_point_2m', 'apparent_temperature', 'temperature_80m', 'temperature_120m', 'soil_temperature_0cm', 'soil_temperature_6cm']
    for i in range(len(temperature)):
        df_hourly[temperature[i]] = (df_hourly[temperature[i]] - 32) * 5 / 9
    # Переводим узлы в м/c
    speed=['ind_speed_10m', 'wind_speed_80m']
    for i in range(len(speed)):
        df_hourly[speed[i]] = df_hourly[speed[i]] * 1.852 / 3.6
    # Переводим футы в метры
    df_hourly['visibility'] = df_hourly['visibility'] * 3.048
    # Переводим дюймы в миллиметры
    depth=['rain', 'showers', 'snowfall']
    for i in range(len(depth)):
        df_hourly[depth[i]] = df_hourly[depth[i]] * 25.4
    df_hourly=df_hourly.drop(columns=['wind_direction_10m', 'wind_direction_80m', 'evapotranspiration', 'weather_code'])

    df_daily=pd.DataFrame(data['daily'])
    day_time=['time', 'sunrise', 'sunset']
    # Переводим в формат datatime
    for i in range(len(day_time)):
        df_daily[day_time[i]]=pd.to_datetime(df_daily[day_time[i]], unit='s')
    df_daily['daylight_duration']=df_daily['daylight_duration'] / 3600
    # Установим время с которого начинается исчисление суток в рассматриваемой часовой зоне
    df_hourly['date'] = (df_hourly['time'] - pd.Timedelta(hours=17)).dt.floor('D') + pd.Timedelta(hours=17)
    df_daily = df_daily.rename(columns={'time': 'date'})
    # Создадим колонки для хранения среднего значения показателей за период в 24 часа
    avg_24h=['temperature_2m', 'relative_humidity_2m', 'dew_point_2m', 'apparent_temperature',
            'temperature_80m', 'temperature_120m', 'wind_speed_10m', 'wind_speed_80m', 'visibility']
    total_24h=[ 'rain', 'showers', 'snowfall']
    for column_name in avg_24h:
        df_daily[column_name] = np.nan
    for column_name in total_24h:
        df_daily[column_name] = np.nan
    # Заполним созданные колонки агрегированными данными
    end_index = 0
    for i in range(len(df_daily)):
        if end_index >= len(df_hourly):
            break
        start_index = end_index
        while end_index < len(df_hourly) and df_daily.at[i, 'date'] == df_hourly.at[end_index, 'date']:
            end_index += 1
        if start_index == end_index:
            continue
        for column_name in avg_24h:
            df_daily.at[i, column_name] = df_hourly[column_name].iloc[start_index:end_index].mean()
        for column_name in total_24h:
            df_daily.at[i, column_name] = df_hourly[column_name].iloc[start_index:end_index].sum()
    df_daily = df_daily.rename(columns={
        'temperature_2m': 'avg_temperature_2m_24h',
        'relative_humidity_2m': 'avg_relative_humidity_2m_24h',
        'dew_point_2m': 'avg_dew_point_2m_24h',
        'apparent_temperature': 'avg_apparent_temperature_24h',
        'temperature_80m': 'avg_temperature_80m_24h',
        'temperature_120m': 'avg_temperature_120m_24h',
        'wind_speed_10m': 'avg_wind_speed_10m_24h',
        'wind_speed_80m': 'avg_wind_speed_80m_24h',
        'rain': 'total_rain_24h',
        'showers': 'total_showers_24h',
        'snowfall': 'total_snowfall_24h',
        'visibility': 'avg_visibility_24h'
        })
    # Округлим значения времени восхода и времени заката для определения светового дня
    for column_name in ['sunrise_round', 'sunset_round']:
        df_daily[column_name] = pd.NaT
    for i in range(len(df_daily)):
        df_daily.at[i, 'sunrise_round']=df_daily.at[i, 'sunrise'].ceil('h')
        df_daily.at[i, 'sunset_round']=df_daily.at[i, 'sunset'].floor('h')
    # Создадим колонки для хранения среднего значения показателей за период в световой день
    avg_daylight=['temperature_2m', 'relative_humidity_2m', 'dew_point_2m', 'apparent_temperature',
                'temperature_80m', 'temperature_120m', 'wind_speed_10m', 'wind_speed_80m', 'visibility']
    total_daylight=[ 'rain', 'showers', 'snowfall']
    for column_name in avg_daylight:
        df_daily[column_name] = np.nan
    for column_name in total_daylight:
        df_daily[column_name] = np.nan
    # Заполним созданные колонки агрегированными данными
    end_index = 0
    for i in range(len(df_daily)):
        if end_index >= len(df_hourly):
            break
        while df_daily.at[i, 'sunrise_round'] != df_hourly.at[end_index, 'time']:
            end_index+=1
        start_index = end_index
        while end_index < len(df_hourly) and (df_hourly.at[end_index, 'time']>=df_daily.at[i, 'sunrise_round']) and (df_hourly.at[end_index, 'time']<=df_daily.at[i, 'sunset_round']):
            end_index += 1
        if start_index == end_index:
            continue
        for column_name in avg_daylight:
            df_daily.at[i, column_name] = df_hourly[column_name].iloc[start_index:end_index].mean()
        for column_name in total_daylight:
            df_daily.at[i, column_name] = df_hourly[column_name].iloc[start_index:end_index].sum()
    # Переименуем колонки согласно заданию
    df_daily = df_daily.rename(columns={
        'temperature_2m': 'avg_temperature_2m_daylight',
        'relative_humidity_2m': 'avg_relative_humidity_2m_daylight',
        'dew_point_2m': 'avg_dew_point_2m_daylight',
        'apparent_temperature': 'avg_apparent_temperature_daylight',
        'temperature_80m': 'avg_temperature_80m_daylight',
        'temperature_120m': 'avg_temperature_120m_daylight',
        'wind_speed_10m': 'avg_wind_speed_10m_daylight',
        'wind_speed_80m': 'avg_wind_speed_80m_daylight',
        'rain': 'total_rain_daylight',
        'showers': 'total_showers_daylight',
        'snowfall': 'total_snowfall_daylight',
        'visibility': 'avg_visibility_daylight'
        }).drop(columns=['sunrise_round', 'sunset_round'])

    df_hourly = df_hourly.rename(columns={
        'wind_speed_10m': 'wind_speed_10m_m_per_s',
        'wind_speed_80m': 'wind_speed_80m_m_per_s',
        'temperature_2m': 'temperature_2m_celsius',
        'apparent_temperature': 'apparent_temperature_celsius',
        'temperature_80m': 'temperature_80m_celsius',
        'temperature_120m': 'temperature_120m_celsius',
        'soil_temperature_0cm': 'soil_temperature_0cm_celsius',
        'soil_temperature_6cm': 'soil_temperature_6cm_celsius',
        'rain': 'rain_mm',
        'showers': 'showers_mm',
        'snowfall': 'snowfall_mm'}).drop(columns=['relative_humidity_2m', 'dew_point_2m', 'visibility'])
    
    # Создадим результатирующий датафрейм через слияние двух вспомогательных
    result = df_hourly.merge(
        df_daily,
        on='date',
        how='left')

    result = result.rename(columns={
        'daylight_duration': 'daylight_hours',
        'sunset': 'sunset_iso',
        'sunrise': 'sunrise_iso'
    }).drop(columns=['date'])
    # Зададим временные данные в формате ISO 8601
    result['sunrise_iso'] = result['sunrise_iso'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    result['sunset_iso'] = result['sunset_iso'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    result=result.reindex(columns=[ 'time', 'avg_temperature_2m_24h', 'avg_relative_humidity_2m_24h', 
                                'avg_dew_point_2m_24h', 'avg_apparent_temperature_24h',
                                'avg_temperature_80m_24h', 'avg_temperature_120m_24h',
                                'avg_wind_speed_10m_24h', 'avg_wind_speed_80m_24h',
                                'avg_visibility_24h', 'total_rain_24h', 'total_showers_24h',
                                'total_snowfall_24h', 'avg_temperature_2m_daylight',
                                'avg_relative_humidity_2m_daylight', 'avg_dew_point_2m_daylight',
                                'avg_apparent_temperature_daylight', 'avg_temperature_80m_daylight',
                                'avg_temperature_120m_daylight', 'avg_wind_speed_10m_daylight',
                                'avg_wind_speed_80m_daylight', 'avg_visibility_daylight',
                                'total_rain_daylight', 'total_showers_daylight',
                                'total_snowfall_daylight', 'wind_speed_10m_m_per_s',
                                'wind_speed_80m_m_per_s', 'temperature_2m_celsius',
                                'apparent_temperature_celsius', 'temperature_80m_celsius',
                                'temperature_120m_celsius', 'soil_temperature_0cm_celsius',
                                'soil_temperature_6cm_celsius', 'rain_mm', 'showers_mm', 'snowfall_mm',
                                'daylight_hours', 'sunset_iso', 'sunrise_iso'])
    
    return result
# Функция загрузки итоговых данных в файл формата .csv
def load_to_csv(df: pd.DataFrame, file_path: str) -> None:
    result_csv=df.drop(columns=['time'])
    result_csv.to_csv(file_path)
# Функция загрузки итоговых данных в базу данных
def load_to_db(df: pd.DataFrame) -> None:
    # Создаем новую сессию
    db = SessionLocal()
    try:
        # Конвертируем DataFrame в список словарей
        data = df.to_dict('records')
        # Вставляем новые данные с обработкой дубликатов по атрибуту "time"
        upsert_dup = insert(WeatherRecords).values(data)
        upsert_dup = upsert_dup.on_conflict_do_update(
            index_elements=['time'],  
            set_={ 
               column_name: column_value  
               for column_name, column_value in upsert_dup.excluded.items()
               if column_name not in ['time']  })
        db.execute(upsert_dup)
        db.commit()
    # Откатваем базу данных к предыдущему состоянию при возникновении ошибок и выводим ошибку
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
# Функция вставки данных из API в файл формата .csv
def inserting_api_to_csv(start_date: str, end_date: str, path_to_save: str) -> None:
    data=extract(start_date, end_date)
    result_df=transform(data)
    load_to_csv(result_df, path_to_save)
# Функция вставки данных из API в базу данных
def inserting_api_to_db(start_date: str, end_date: str) -> None:
    data=extract(start_date, end_date)
    result_df=transform(data)
    load_to_db(result_df)
# Функция вставки данных из json в файл формата .csv
def inserting_json_to_csv(json_file: str, path_to_save: str) -> None:
    with open(json_file) as file:
        data=json.load(file)
    result_df=transform(data)
    load_to_csv(result_df, path_to_save)
# Функция вставки данных из json в базу данных
def inserting_json_to_db(json_file: str) -> None:
    with open(json_file) as file:
        data=json.load(file)
    result_df=transform(data)
    load_to_db(result_df)
# Функция проверки формата входной даты
def is_valid_date(date: str) -> bool:
    try:
        datetime.strptime(date, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
def main():
    # Выводим инструкции по работе с ETL-приложением при отсутствии команд
    if len(sys.argv) < 2:
        print("Доступные опции ETL-приложения:")
        print(" - для выгрузки данных по API и их вставки в файл формата .csv используйте команду " \
        "'api_to_csv <start_date> <end_date> <path_to_save>' где аргументы <start_date> и <end_date> задают" \
        "временной интервал (формат YYYY-MM-DD), по котрому будет осуществляться выгрузка данных, " \
        "<path_to_save> - путь, по которому будет храниться файл формата .csv в проекте")
        print(" - для выгрузки данных по API и их вставки в базу данных используйте команду " \
        "'api_to_db <start_date> <end_date>' где аргументы <start_date> и <end_date> задают" \
        "временной интервал (формат YYYY-MM-DD), по котрому будет осуществляться выгрузка данных")
        print(" - для выгрузки данных из json и их вставки в файл формата .csv используйте команду " \
        "'json_to_csv <file_json> <path_to_save>' где аргумент <file_json> задает" \
        "путь, где хранится используемый для извлечения данных json-файл в проекте, " \
        "<path_to_save> - путь, по которому будет храниться файл формата .csv в проекте")
        print(" - для выгрузки данных из json и их вставки в базу данных используйте команду " \
        "'json_to_csv <file_json>' где аргумент <file_json> задает путь, где хранится используемый для извлечения " \
        "данных json-файл в проекте")
        return
    
    # Обрабатывем вводимые пользователем команды для выполнения функционала приложения
    command=sys.argv[1]
    try:
        if command =='api_to_csv':
            start_date, end_date, path_to_save = sys.argv[2], sys.argv[3], sys.argv[4]
            if not (is_valid_date(start_date) and is_valid_date(end_date)):
                print("Ошибка ввода данных! Обе даты должны быть записаны в формате ГГГГ-ММ-ДД")
                sys.exit(1)
            inserting_api_to_csv(start_date, end_date, path_to_save)
            print(f"Данные успешно сохранены в {path_to_save}!")
        elif command == 'api_to_db':
            start_date, end_date = sys.argv[2], sys.argv[3]
            if not (is_valid_date(start_date) and is_valid_date(end_date)):
                print("Ошибка ввода данных! Обе даты должны быть записаны в формате ГГГГ-ММ-ДД")
                sys.exit(1)
            inserting_api_to_db(start_date, end_date)
            print(f"Данные успешно сохранены в базу данных!")
        elif command == 'json_to_csv':
            file_json, path_to_save = sys.argv[2], sys.argv[3]
            inserting_json_to_csv(file_json, path_to_save)
            print(f"Данные успешно сохранены в {path_to_save}!")
        elif command == 'json_to_db':
            file_json = sys.argv[2]
            inserting_json_to_db(file_json)
            print(f"Данные успешно сохранены в базу данных!")
        else:
            print("Неизвестная команда! Пожалуйста, ознакомтесь со списком всех опций данного ETL-приложения:")
            print(" - для выгрузки данных по API и их вставки в файл формата .csv используйте команду " \
            "'api_to_csv <start_date> <end_date> <path_to_save>' где аргументы <start_date> и <end_date> задают" \
            "временной интервал (формат YYYY-MM-DD), по котрому будет осуществляться выгрузка данных, " \
            "<path_to_save> - путь, по которому будет храниться файл формата .csv в проекте")
            print(" - для выгрузки данных по API и их вставки в базу данных используйте команду " \
            "'api_to_db <start_date> <end_date>' где аргументы <start_date> и <end_date> задают" \
            "временной интервал (формат YYYY-MM-DD), по котрому будет осуществляться выгрузка данных")
            print(" - для выгрузки данных из json и их вставки в файл формата .csv используйте команду " \
            "'json_to_csv <file_json> <path_to_save>' где аргумент <file_json> задает" \
            "путь, где хранится используемый для извлечения данных json-файл в проекте, " \
            "<path_to_save> - путь, по которому будет храниться файл формата .csv в проекте")
            print(" - для выгрузки данных из json и их вставки в базу данных используйте команду " \
            "'json_to_csv <file_json>' где аргумент <file_json> задает путь, где хранится используемый для извлечения " \
            "данных json-файл в проекте")

    except Exception as e:
            print(f"Ошибка отработки приложения: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()