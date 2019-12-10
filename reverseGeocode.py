from queue import Queue
from threading import Thread
from urllib.error import URLError
from urllib.request import urlopen, quote
import csv
import json
import re
from itertools import islice
import logging
from requests import HTTPError

urls = []
def getCsvData(filename):
    with open(filename, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for line in csv_reader:
            urls.append("https://pcmiler.alk.com/apis/rest/v1.0/Service.svc/locations/reverse?Coords=" + line['Lon'] + ',' + line['Lat'] + "&matchNamedRoadsOnly=true&dataset=Current&authToken=7F2255D520884543ACD86FC5CF1D8A62")


def crawl(q, result):
    while not q.empty():
        work = q.get()
        try:
            data = urlopen((work[1])).read()
            logging.info("Requested..." + work[1])
            print("Requested..." + work[1])
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
            result[work[0]] = {}
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
            result[work[0]] = {}
        else:
            result[work[0]] = data

        q.task_done()
    return True



def getData(results):
    ans = []

    csv_columns = ['Trimble_MAPS_StreetAddress', 'Trimble_MAPS_City','Trimble_MAPS_State','Trimble_MAPS_PostalCode']
    print(len(results))
    for i in range(len(results)):
        obj = []
        data = json.loads(results[i])

        StreetAddress = data["Address"]["StreetAddress"]
        City = data["Address"]["City"]
        State = data["Address"]["State"]
        PostalCode = data["Address"]["Zip"]


        # try:
        # #if (bool(re.search(r'\d+', Street).group())): house = int(re.search(r'\d+', Street).group())
        #     house = (re.search(r'\d+', Street1).group())
        #     Street2 = Street1.replace(house, '')
        # except:
        #     house = "Not Found"
        #     Street2 = Street1
        #print(house)
        #print(Street2)


        # Country = data["Address"]["Country"]

        # StreetAddressOriginal = Street1 + Zip + City + Country

        # obj.append(StreetAddressOriginal)

        obj.append(StreetAddress)
        obj.append(City)
        obj.append(State)
        obj.append(PostalCode)


        ans.append(obj)
        print(ans)

    #Change the name of the output csv file before running the code ow previous file will be overridden.
    filename = "FPARUN.csv"
    with open(filename, 'w', encoding="utf-8") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(csv_columns)
        csvwriter.writerows(ans)



def main():


    in_csv="FPA-lat.csv"
    getCsvData(in_csv)

    q = Queue(maxsize=0)
    num_theads = min(50, len(urls))

    for i in range(len(urls)):
        q.put((i, urls[i]))

    results = [{} for x in urls]

    for i in range(num_theads):
        logging.debug('Starting thread ', i)
        print('Starting thread ', i)
        worker = Thread(target=crawl, args=(q, results))
        worker.setDaemon(
            True)  # setting threads as "daemon" allows main program to exit eventually even if these dont finish correctly.
        worker.start()

    q.join()  # now we wait until the queue has been processed
    getData(results)

if __name__ == "__main__":
    main()