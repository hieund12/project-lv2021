import csv
import os
import aqi
import pandas as pd
from pandas import DataFrame


def ReadCSVasList(csv_file):
    try:
        with open(csv_file) as csvfile:
            reader = csv.DictReader(csvfile, dialect="excel")
            datalist = []
            for row in reader:
                datalist.append([int(row['pm25']), int(row['pm10']), int(row['no2']), int(row['co']), int(row['so2']),
                                 float(row['o3'])])
            return datalist
    except IOError as err:
        print("I/O error({0})".format(err))
    return


currentPath = os.getcwd()
csv_file = currentPath + "/2020-03-01_sang.csv"
DataList = ReadCSVasList(csv_file)

aqi_list = []  # to stuff the resulting meta-data for images
for i in DataList:
    # print(i[5])
    myaqi = round(aqi.to_aqi([
        (aqi.POLLUTANT_PM25, i[0]),
        (aqi.POLLUTANT_PM10, i[1]),
        (aqi.POLLUTANT_NO2_1H, i[2]),
        (aqi.POLLUTANT_CO_8H, i[3]),
        (aqi.POLLUTANT_SO2_1H, i[4]),
        (aqi.POLLUTANT_O3_8H, i[5])
    ]))
    aqi_list.append(myaqi)

AQI = []
# https://taqm.epa.gov.tw/taqm/en/b0201.aspx
for n in aqi_list:
    if 0 <= int(n) <= 50:
        AQI.append('Good')
    elif 51 <= int(n) <= 100:
        AQI.append('Moderate')
    elif 101 <= int(n) <= 150:
        AQI.append('Unhealthy for Sensitive Groups')
    elif 151 <= int(n) <= 200:
        AQI.append('Unhealthy')
    elif 201 <= int(n) <= 300:
        AQI.append('Very Unhealthy')
    elif 300 <= int(n):
        AQI.append('Hazardous')

dfn = DataFrame(AQI, columns=['aqi'])
dataset = pd.read_csv('2020-03-01_sang.csv')
# Get format for time series
df = DataFrame(dataset, columns=['time', 'lon', 'lat', 'hum', 'tem',
                                 'fah', 'pm25', 'pm10', 'uv', 'no2', 'co', 'so2', 'o3'])
dfnitro = pd.merge(df, dfn, left_index=True, right_index=True, how='outer')
dfnitro.to_csv('data_aqi_03_01_sang.csv')
