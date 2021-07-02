import os
import os.path
import sys
import warnings
from math import radians, cos, sin, asin, sqrt
import pandas as pd
from DrawMap import draw_map
from numpy import amax, amin

warnings.filterwarnings('ignore')


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000


def mean_nonzero(array_input):
    sum_values = 0
    number_zero = 0
    for index in range(len(array_input)):
        if array_input[index] == 0:
            number_zero = number_zero + 1
        else:
            sum_values = sum_values + array_input[index]

    if number_zero == len(array_input):
        return 0
    else:
        return sum_values / (len(array_input) - number_zero)


def parameter_validation(path, year, group, day):
    # path
    if not os.path.isdir(path):
        print('This path not exist:', path)
        return False
    # year
    elif year != 2019 and year != 2018:
        print('This year invalid:', year)
        return False
    # group
    elif group > 5 or group < 1:
        print('This group invalid:', group)
        return False
    # day
    elif year == 2019 and (day > 2 or day < 1):
        print('This day invalid:', day)
        return False


# 2019: 5 people in one group (1->5), (6->10), (11->15), (16->20), and (21->25).
#   ID = 101 means that personID = 1, be a volunteer in the first day.
#   ID = 201 means that personID = 1, be a volunteer in the second day.
# 2018: 6 people in one group (1->6), (7->12), (13->18), (19->24), and (25->30).
def GetFilesCSV(path, year, group, day):
    all_files = []
    scope_file_name = []

    # Check parameter input
    if parameter_validation(path, year, group, day) is False:
        sys.exit()

    # Create rule file for getting
    if year == 2019:
        people_in_group = 5
        min_id_in_group = people_in_group * group - people_in_group + 1
        max_id_in_group = people_in_group * group + 1
        for userID in range(min_id_in_group, max_id_in_group):
            # name file = day+uesID_2019.csv (e.g. '101_2019.csv')
            scope_file_name.append(str(day) + str(userID).zfill(2) + '_' + str(year) + '.csv')
    elif year == 2018:
        people_in_group = 6
        min_id_in_group = people_in_group * group - people_in_group + 1
        max_id_in_group = people_in_group * group + 1
        for userID in range(min_id_in_group, max_id_in_group):
            # name file = uesID_2019.csv (e.g. '10_2018.csv')
            scope_file_name.append(str(userID).zfill(2) + '_' + str(year) + '.csv')

    # Get all files based on year and group
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv') and file in scope_file_name:
                all_files.append(os.path.join(root, file))
    return all_files


def PreprocessingData(path, year, group, day=1):
    use_cols = ['start_datetime', 'longitude', 'lattitude', 'pm2_5', 'temperature', 'humidity']
    # Get all files
    all_files = GetFilesCSV(path, year, group, day)
    print(all_files)
    # Read data set
    dataset = (pd.read_csv(file, encoding='unicode_escape', usecols=use_cols) for file in all_files)
    # Concatenate pandas objects along a particular axis
    dataset = pd.concat(dataset, ignore_index=False)

    # Remove row not used
    dataset = dataset[dataset.longitude != 0]
    dataset = dataset[dataset.lattitude != 0]
    dataset = dataset[dataset.temperature != 0]
    dataset = dataset[dataset.humidity != 0]
    dataset.drop(['temperature', 'humidity'], axis=1, inplace=True)
    # dataset.drop_duplicates(subset='longitude', keep='first', inplace=True)
    return dataset


def get_coordinates_test(folder, test_range):
    file_csv = 'data' + '\\' + folder + '\\' + test_range.split(',')[0] + '\\' + test_range.split(',')[1] + '.csv'
    start_time = test_range.split(',')[2]
    end_time = test_range.split(',')[3]

    df = pd.read_csv(file_csv, usecols=['start_datetime', 'end_datetime', 'longitude', 'lattitude', 'temperature'])
    df.set_index(['start_datetime'], inplace=True)

    if '2019' in file_csv:
        end_time = str(pd.to_datetime(end_time, format='%Y-%m-%d %H:%M:%S') + pd.offsets.Minute(-1))
    elif '2018' in file_csv:
        df = df[df.temperature != 0]

    df = df[start_time:end_time]
    df.drop(['temperature'], axis=1, inplace=True)

    df.reset_index(inplace=True)
    coordinates_test_list = []
    for index in range(df.shape[0]):
        coordinates_test_list.append([df.longitude[index], df.lattitude[index]])
    return start_time, end_time, coordinates_test_list


def get_station_data(path, time_info):
    all_files = []
    use_cols = ['year', 'month', 'day', 'hour', 'PM2.5(ug/m3)', 'longitude', 'latitude']
    start_time = pd.to_datetime(time_info, format='%Y-%m-%d %H:%M:%S')

    # Get all files based on year and group
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):
                all_files.append(os.path.join(root, file))

    dataset = (pd.read_csv(file, encoding='unicode_escape', usecols=use_cols) for file in all_files)
    dataset = pd.concat(dataset, ignore_index=False)
    dataset = dataset.loc[dataset.year == start_time.year]
    dataset = dataset.loc[dataset.month == start_time.month]
    dataset = dataset.loc[dataset.day == start_time.day]
    dataset = dataset.loc[dataset.hour == start_time.hour]
    dataset.columns = ['year', 'month', 'day', 'hour', 'pm2_5', 'longitude', 'lattitude']
    dataset = dataset.loc[dataset.pm2_5.notna()]
    dataset.pm2_5 = pd.to_numeric(dataset.pm2_5, downcast='signed')
    return dataset[['longitude', 'lattitude', 'pm2_5']]


def main():
    # Input data set
    start_time, end_time, coordinates_test_list = get_coordinates_test('Testing',
    'Q10,103_2019,2019-03-23 12:00:00+09,2019-03-23 12:30:00+09')
    dataset = PreprocessingData('data\\', 2019, 1, 1)

    dataset = dataset[dataset.start_datetime >= start_time]
    dataset = dataset[dataset.start_datetime <= end_time]

    # dataset.to_csv('example.csv')
    station_data = get_station_data('data\\Station', start_time)
    dataset = pd.concat([dataset, station_data])

    dataset.sort_values('start_datetime', ascending=True, inplace=True)

    # Remove coordinates predict in data
    for i in range(len(coordinates_test_list)):
        dataset = dataset[(dataset.longitude + dataset.lattitude) !=
                          (coordinates_test_list[i][0] + coordinates_test_list[i][1])]

    dataset.reset_index(inplace=True)

    dataset.to_csv('example.csv', index=True)
    dataset = pd.read_csv('example.csv')

    print('\r\nLength predict =', len(coordinates_test_list), 'values')
    print('Size of data set =', dataset.shape[0], 'rows\r\n')
    predicted = []
    predicted1 = []
    predicted2 = []
    unit_loop = 0
    max_radius = 100  # 100 met
    ideal_radius = 20  # 20 met

    # Show map
    draw_map(dataset['longitude'].values, dataset['lattitude'].values, 'cornflowerblue', 'map14', 17, ideal_radius,
             [i[0] for i in coordinates_test_list], [i[1] for i in coordinates_test_list])

    # Execute
    for coordinates_test in coordinates_test_list:
        pm2_5_list = []
        index_list = []
        unit_loop = unit_loop + 1
        print('Lon_input =', coordinates_test[0], 'Lat_input =', coordinates_test[1])
        for radius in range(1, max_radius + 1):
            flag_radius_break = False
            for index in range(dataset.shape[0]):
                coordinates = ([float(str(dataset['longitude'].values[index])),
                                float(str(dataset['lattitude'].values[index]))])
                calculator = haversine(coordinates_test[0], coordinates_test[1], coordinates[0], coordinates[1])
                if calculator <= radius and index not in index_list:
                    pm2_5_list.append(dataset['pm2_5'].values[index])
                    index_list.append(index)
                    print('Row =', index, 'Radius =', str(radius),
                          'lon_output =', dataset['longitude'].values[index],
                          'lat_output =', dataset['lattitude'].values[index],
                          'pm2_5 =', dataset['pm2_5'].values[index])
                if radius > ideal_radius and len(pm2_5_list) > 0:
                    predicted.append(int(amax(pm2_5_list)))
                    predicted1.append(int(mean_nonzero(pm2_5_list)))
                    predicted2.append(int(amin(pm2_5_list)))
                    print('pm2_5_predicted:', int(amax(pm2_5_list)))
                    flag_radius_break = True
                    break

            if flag_radius_break is True:
                break
        if len(predicted) < unit_loop:
            predicted.append(0)
            predicted1.append(0)
            predicted2.append(0)
        print(unit_loop, '>=======================================================> END\r\n')

    print('Predicted_Max =', predicted)
    print('Predicted_Average =', predicted1)
    print('Predicted_Min =', predicted2)


if __name__ == '__main__':
    main()