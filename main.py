import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
import time

# List of provinces in Vietnam with latitude and longitude
provinces = [
    {"name": "Hà Nội", "latitude": 21.0285, "longitude": 105.8544},
    {"name": "TP. Hồ Chí Minh", "latitude": 10.7769, "longitude": 106.7009},
    {"name": "Đà Nẵng", "latitude": 16.0544, "longitude": 108.2022},
    {"name": "Hải Phòng", "latitude": 20.8449, "longitude": 106.6881},
    {"name": "Cần Thơ", "latitude": 10.0452, "longitude": 105.7469},
    {"name": "An Giang", "latitude": 10.5216, "longitude": 105.1259},
    {"name": "Bà Rịa - Vũng Tàu", "latitude": 10.3450, "longitude": 107.0843},
    {"name": "Bắc Giang", "latitude": 21.2730, "longitude": 106.1946},
    {"name": "Bắc Kạn", "latitude": 22.1445, "longitude": 105.8348},
    {"name": "Bắc Ninh", "latitude": 21.1861, "longitude": 106.0763},
    {"name": "Bến Tre", "latitude": 10.2415, "longitude": 106.3759},
    {"name": "Bình Dương", "latitude": 11.3250, "longitude": 106.6630},
    {"name": "Bình Định", "latitude": 13.7820, "longitude": 109.2192},
    {"name": "Bình Phước", "latitude": 11.7512, "longitude": 106.9190},
    {"name": "Bình Thuận", "latitude": 11.0956, "longitude": 108.2172},
    {"name": "Cao Bằng", "latitude": 22.6659, "longitude": 106.2571},
    {"name": "Đắk Lắk", "latitude": 12.6660, "longitude": 108.0383},
    {"name": "Đắk Nông", "latitude": 12.2646, "longitude": 107.6098},
    {"name": "Điện Biên", "latitude": 21.3843, "longitude": 103.0163},
    {"name": "Đồng Nai", "latitude": 11.0590, "longitude": 107.0708},
    {"name": "Đồng Tháp", "latitude": 10.6125, "longitude": 105.6882},
    {"name": "Gia Lai", "latitude": 13.9774, "longitude": 108.0132},
    {"name": "Hà Giang", "latitude": 22.8324, "longitude": 104.9842},
    {"name": "Hà Nam", "latitude": 20.5311, "longitude": 105.9067},
    {"name": "Hà Tĩnh", "latitude": 18.3550, "longitude": 105.8873},
    {"name": "Hậu Giang", "latitude": 9.7846, "longitude": 105.4701},
    {"name": "Hòa Bình", "latitude": 20.8153, "longitude": 105.3376},
    {"name": "Hưng Yên", "latitude": 20.8526, "longitude": 106.0161},
    {"name": "Khánh Hòa", "latitude": 12.2388, "longitude": 109.1967},
    {"name": "Kiên Giang", "latitude": 10.0126, "longitude": 105.0809},
    {"name": "Kon Tum", "latitude": 14.3498, "longitude": 108.0000},
    {"name": "Lai Châu", "latitude": 22.3964, "longitude": 103.4582},
    {"name": "Lâm Đồng", "latitude": 11.9404, "longitude": 108.4583},
    {"name": "Lạng Sơn", "latitude": 21.8479, "longitude": 106.7570},
    {"name": "Lào Cai", "latitude": 22.4800, "longitude": 103.9707},
    {"name": "Long An", "latitude": 10.5420, "longitude": 106.4055},
    {"name": "Nam Định", "latitude": 20.4286, "longitude": 106.1621},
    {"name": "Nghệ An", "latitude": 19.2333, "longitude": 104.9200},
    {"name": "Ninh Bình", "latitude": 20.2530, "longitude": 105.9740},
    {"name": "Ninh Thuận", "latitude": 11.6739, "longitude": 108.9703},
    {"name": "Phú Thọ", "latitude": 21.3256, "longitude": 105.2050},
    {"name": "Phú Yên", "latitude": 13.0891, "longitude": 109.0929},
    {"name": "Quảng Bình", "latitude": 17.4687, "longitude": 106.6223},
    {"name": "Quảng Nam", "latitude": 15.5735, "longitude": 108.4740},
    {"name": "Quảng Ngãi", "latitude": 15.1205, "longitude": 108.7935},
    {"name": "Quảng Ninh", "latitude": 21.0064, "longitude": 107.2925},
    {"name": "Quảng Trị", "latitude": 16.8163, "longitude": 107.0953},
    {"name": "Sóc Trăng", "latitude": 9.6025, "longitude": 105.9712},
    {"name": "Sơn La", "latitude": 21.3276, "longitude": 103.9144},
    {"name": "Tây Ninh", "latitude": 11.3445, "longitude": 106.1349},
    {"name": "Thái Bình", "latitude": 20.4463, "longitude": 106.3402},
    {"name": "Thái Nguyên", "latitude": 21.5675, "longitude": 105.8253},
    {"name": "Thanh Hóa", "latitude": 19.8006, "longitude": 105.7763},
    {"name": "Thừa Thiên Huế", "latitude": 16.4637, "longitude": 107.5909},
    {"name": "Tiền Giang", "latitude": 10.4493, "longitude": 106.3420},
    {"name": "Trà Vinh", "latitude": 9.9347, "longitude": 106.3454},
    {"name": "Tuyên Quang", "latitude": 21.8170, "longitude": 105.2171},
    {"name": "Vĩnh Long", "latitude": 10.2543, "longitude": 105.9720},
    {"name": "Vĩnh Phúc", "latitude": 21.3090, "longitude": 105.6049},
    {"name": "Yên Bái", "latitude": 21.7055, "longitude": 104.8918},
    {"name": "Cà Mau", "latitude": 9.1768, "longitude": 105.1524},
    {"name": "Hải Dương", "latitude": 20.9374, "longitude": 106.3146},
    {"name": "Bạc Liêu", "latitude": 9.2941, "longitude": 105.7278},
]

count = 0
total_provinces = len(provinces)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"

def get_weather_data(province, start_date, end_date):
    params = {
	"latitude": province["latitude"],
	"longitude": province["longitude"],
	"start_date": start_date,
	"end_date": end_date,
	"daily": ["rain_sum", "temperature_2m_mean", "pressure_msl_mean", "relative_humidity_2m_mean", "wind_speed_10m_mean", "soil_moisture_0_to_100cm_mean", "surface_pressure_mean", "temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max"],
	"timezone": "Asia/Bangkok"
    }

    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
    
    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_rain_sum = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()
    daily_pressure_msl_mean = daily.Variables(2).ValuesAsNumpy()
    daily_relative_humidity_2m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_wind_speed_10m_mean = daily.Variables(4).ValuesAsNumpy()
    daily_soil_moisture_0_to_100cm_mean = daily.Variables(5).ValuesAsNumpy()
    daily_surface_pressure_mean = daily.Variables(6).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(7).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(8).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(9).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
	    start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	    end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	    freq = pd.Timedelta(seconds = daily.Interval()),
	    inclusive = "left"
    )}

    daily_data["rain_sum"] = daily_rain_sum
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["pressure_msl_mean"] = daily_pressure_msl_mean
    daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
    daily_data["wind_speed_10m_mean"] = daily_wind_speed_10m_mean
    daily_data["soil_moisture_0_to_100cm_mean"] = daily_soil_moisture_0_to_100cm_mean
    daily_data["surface_pressure_mean"] = daily_surface_pressure_mean
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

    daily_dataframe = pd.DataFrame(data = daily_data)

    csv_filename = f"{province['name']}.csv"

    # Save the dataframe to a CSV file
    daily_dataframe.to_csv(csv_filename, index=False)

for province in provinces:
    print(f"Fetching weather data for {province['name']}...")
    get_weather_data(province, "2000-01-01", "2009-12-31")  # Adjust the date range as needed here
    count += 1
    print(f"Processed {count}/{total_provinces} provinces")
    time.sleep(10)  # Sleep for 10 seconds to avoid hitting the API rate limit





