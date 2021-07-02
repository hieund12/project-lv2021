import csv
import os
import requests
from multiprocessing.pool import ThreadPool


def download_url(url):
    print("downloading: ", url)
    file_name_start_pos = url.rfind("/") + 1
    file_name = url[file_name_start_pos:]

    r = requests.get(url, stream=True)
    if r.status_code == requests.codes.ok:
        with open(file_name, 'wb') as f:
            for data in r:
                f.write(data)
    return url


def ReadCSVasList(csv_file):
    try:
        with open(csv_file) as csvfile:
            reader = csv.DictReader(csvfile, dialect="excel")
            datalist = []
            for row in reader:
                datalist.append(str(row['image']))
            return datalist
    except IOError as err:
        print("I/O error({0})".format(err))
    return


currentPath = os.getcwd()
csv_file = currentPath + "\\data.csv"
DataList = ReadCSVasList(csv_file)

# Run 5 multiple threads. Each call will take the next element in urls list
results = ThreadPool(5).imap_unordered(download_url, DataList)
for r in results:
    print(r)

