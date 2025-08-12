from sqlalchemy import create_engine, Column, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Задаем url для подключения к postgresql базе данных
DATABASE_URL = "postgresql://anya_shcherba:Qq123456@db:5432/weather_db"

# Создаем движок sqlalchemy для работы с бд
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Используем базовый класс для построения всех моделей
Base = declarative_base()

# Задаем модель для собрания показателей о погоде
class WeatherRecords(Base):
    __tablename__ = "weather_records"
    
    time = Column(DateTime, primary_key=True)
    avg_temperature_2m_24h = Column(Float)
    avg_relative_humidity_2m_24h = Column(Float)
    avg_dew_point_2m_24h = Column(Float)
    avg_apparent_temperature_24h = Column(Float)
    avg_temperature_80m_24h = Column(Float)
    avg_temperature_120m_24h = Column(Float)
    avg_wind_speed_10m_24h = Column(Float)
    avg_wind_speed_80m_24h = Column(Float)
    avg_visibility_24h = Column(Float)
    total_rain_24h = Column(Float)
    total_showers_24h = Column(Float)
    total_snowfall_24h = Column(Float)
    avg_temperature_2m_daylight = Column(Float)
    avg_relative_humidity_2m_daylight = Column(Float)
    avg_dew_point_2m_daylight = Column(Float)
    avg_apparent_temperature_daylight = Column(Float)
    avg_temperature_80m_daylight = Column(Float)
    avg_temperature_120m_daylight = Column(Float)
    avg_wind_speed_10m_daylight = Column(Float)
    avg_wind_speed_80m_daylight = Column(Float)
    avg_visibility_daylight = Column(Float)
    total_rain_daylight = Column(Float)
    total_showers_daylight = Column(Float)
    total_snowfall_daylight = Column(Float)
    wind_speed_10m_m_per_s = Column(Float)
    wind_speed_80m_m_per_s = Column(Float)
    temperature_2m_celsius = Column(Float)
    apparent_temperature_celsius = Column(Float)
    temperature_80m_celsius = Column(Float)
    temperature_120m_celsius = Column(Float)
    soil_temperature_0cm_celsius = Column(Float)
    soil_temperature_6cm_celsius = Column(Float)
    rain_mm = Column(Float)
    showers_mm = Column(Float)
    snowfall_mm = Column(Float)
    daylight_hours = Column(Float)
    sunset_iso = Column(DateTime)
    sunrise_iso = Column(DateTime)

# Создаем таблицу
Base.metadata.create_all(bind=engine)