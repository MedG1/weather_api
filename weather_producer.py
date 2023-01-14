import json
import time
import urllib.request
from kafka import KafkaProducer

from datetime import date

import ApiParameters
from governorates import governorates

# No api key needed for the API we are working with
# Request URL
url_fetch_historic = "https://archive-api.open-meteo.com/v1/era5?latitude={}&longitude={}&start_date={}&end_date={}&hourly={}" #daily={}&timezone=Europe%2FLondon"


# Creating a producer that will push messages to the registered nodes
producer = KafkaProducer(bootstrap_servers=["localhost:9092", "localhost:9093"])


start_date = "2022-01-08"
end_date = "2023-01-01" #date.today().strftime("%Y-%m-%d")
hourly = "temperature_2m,relativehumidity_2m,apparent_temperature,precipitation,windspeed_10m,winddirection_10m"
daily = "temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,windspeed_10m_max,winddirection_10m_dominant"

# holds previous data
while True:
    for gov,locations in governorates.items():
        for loc, coords in locations.items():
            latitude = coords["lat"]
            longitude = coords["long"]
            url = url_fetch_historic.format(latitude, longitude, start_date, end_date, hourly)
            print(url)
            response = urllib.request.urlopen(url)
            print("request received")
            weather = json.loads(response.read().decode())
            print(weather)
            producer.send("weather", json.dumps(weather).encode())
            print("location = {} ({}, {})".format(loc, latitude, longitude))

    break
print("FINISHED WORK!")