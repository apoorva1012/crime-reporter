import logging
logging.basicConfig(level=logging.DEBUG)

import requests
import json
import itertools
from collections import Counter
from collections import OrderedDict
from datetime import datetime

from spyne import Application, srpc, ServiceBase, Integer, Unicode, Float
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication


class CrimeService(ServiceBase):
    @srpc(Float, Float, Float, _returns=Iterable(Unicode))
    def checkcrime(lat, lon, radius):
        # Convert rpc call to required URL to fetch data
        crime_data = 'https://api.spotcrime.com/crimes.json?lat=' + str(lat) + '&lon=' + str(lon) + '&radius='+ str(radius) + '&key=.'
        # Store response in a variable
        request = requests.get(crime_data).json()
        # Yield the output
        yield churn(request)


# This is the heart of application which churns the request to desired user output
def churn(request):
    addresses = []
    types = []
    times = []
    timeDictionaryOrder = ['12:01am-3am', '3:01am-6am', '6:01am-9am', '9:01am-12pm', '12:01pm-3pm','3:01pm-6pm','6:01pm-9pm','9:01pm-12am']

    # Loop through addresses, types and times of crimes, shred them and append to individual arrays
    for i in range(len(request['crimes'])):
        shredAddress = shred(request['crimes'][i]['address'])
        addresses.append(shredAddress)

        shredTypes = shred(request['crimes'][i]['type'])
        types.append(shredTypes)

        time = datetime.strptime(request['crimes'][i]['date'], "%m/%d/%y %I:%M %p").time() # Extract time
        shredTime = shred(str(time))
        times.append(shredTime)

    # Get count of crimes as per addresses, types and times when they occur
    crimeAddresses = Counter(itertools.chain.from_iterable(addresses))
    topThreeCrimeAddresses = crimeAddresses.most_common(3) #Select top 3 addresses
    crimeAddressDictionary = sortStreets(OrderedDict(topThreeCrimeAddresses))

    crimeTypes = Counter(itertools.chain.from_iterable(types))
    crimeTypesDict = OrderedDict(sort(crimeTypes))

    crimeTimes = Counter(itertools.chain.from_iterable(times))
    crimeTimesDict = OrderedDict((k, crimeTimes[k]) for k in timeDictionaryOrder) #Ordered dictionary as per time

    # Build the result in required format and return it
    result = OrderedDict([('total_crime', len(request['crimes'])),
        ('the_most_dangerous_streets', crimeAddressDictionary),
        ('crime_type_count', crimeTypesDict),
        ('event_time_count', crimeTimesDict)])
    return result


# Shreds any list to return useful result
def shred(array):
    array = array.strip('-') # Few addresses had address ending in '-' character
    array = array.replace(".","") # Few addresses use 'N.'' instead of 'N' like 'N. Mary. vs 'N Mary'
# Address shredding space starts below
    if ' AND ' in array:
        return array.split(' AND ')
    elif '&' in array:
        return [i.strip() for i in array.split('&')]
    elif ' / ' in array:
        return [i.strip() for i in array.split('/')]
    elif ' AT ' in array:
        return [i.strip() for i in array.split(' AT ')][1::]
    elif '-' in array and " " in array:
        splitArray = (array.replace(' ','-',1)).split('-')
        return  splitArray[len(splitArray)-1::]
    # For 3 different occurences of BLOCK
    elif 'BLOCK' in array:
        if 'BLOCK OF' in array:
            return [i.strip() for i in array.split('BLOCK OF')][1::]
        elif 'BLOCK BLOCK' in array:
            return [i.strip() for i in array.split('BLOCK BLOCK')][1::]
        else:
            return [i.strip() for i in array.split('BLOCK')][1::]
    # Time shredding space
    elif ':' in array:
        hour = array.split(':')
        return [timeSlot(hour)] # Returns the time slot list as per hour
    # Added just to ensure all responses are in a common format once they are sent to be shred
    else:
        return array.split('####')


# Converts time to a specific time slot
def timeSlot(hour):
    factor = int(hour[0]) + int(hour[1])
    if 0 < factor <= 3:
        return "12:01am-3am"
    elif factor > 3 and factor <= 6:
        return "3:01am-6am"
    elif factor > 6 and factor <= 9:
        return "6:01am-9am"
    elif factor > 9 and factor <= 12:
        return "9:01am-12pm"
    elif factor > 12 and factor <= 15:
        return "12:01pm-3pm"
    elif factor > 15 and factor <= 18:
        return "3:01pm-6pm"
    elif factor > 18 and factor <= 21:
        return "6:01pm-9pm"
    else:
        return "9:01pm-12am"


# Sorts and returns only streets as per highest no. of crimes
def sortStreets(addresses):
    return [i[0] for i in sorted(addresses.items(), key = lambda item:item[1], reverse=True)]

# Sorts and returns types along with count
def sort(others):
    return [i for i in sorted(others.items(), key = lambda item:item[1], reverse=True)]


application = Application([CrimeService],
    tns='spyne.examples.hello',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('127.0.0.1', 8000, wsgi_app)
    server.serve_forever()
