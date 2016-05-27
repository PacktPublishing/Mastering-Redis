"""Module implements a simple GIS/RestMQ monitoring simulation where messages
are acted upon that contain a weather alert within a 100 km of a Colorado
Ski Mountains.

Usage:

    >>> import app
    >>> app.setup()
    >>> app.monitor_weather("http://localhost:8888") # RestMQ is running locally
    
In a second Python session, a user consumer monitors Aspen Mountain
    >>> import app
    >>> app.weather_alert("http://localhost:8888",
            "1396677600161700")

Sending a Storm Alert warning in a third Python session 
    >>> import json, requests
    >>> location = {"longitude": -106.5, "latitude": 40}
    >>> storm_warning = {"location": location, "alert": "Winter Storm: 10 cm"}
    >>> storm_alert = requests.post("http://localhost:8888/q/monitor",
            data={"value": json.dumps(storm_alert)})

Will generate a message in the second Python session
    >>> Winter Storm: 10 cm
"""
__author__ = "Jeremy Nelson"

import json
import redis
import requests

def setup(datastore=redis.StrictRedis()):
    transaction = datastore.pipeline(transaction=True)
    transaction.execute_command(
        "GEOADD", 
        "colorado_ski_mountains",
        -106.926982, 
        38.905476, 
       "Crested Butte")
    transaction.execute_command(
        "GEOADD", 
        "colorado_ski_mountains",
        -106.3381,
        38.502855,
        "Monarch Mountain")
    transaction.execute_command(
        "GEOADD", 
        "colorado_ski_mountains",
        -106.822146,
        39.165098,
       "Aspen Mountain")
    transaction.execute_command(
        "GEOADD", 
        "colorado_ski_mountains",
        -106.355999,
        39.605234,
        "Vail Mountain")
    transaction.execute()         

def monitor_weather(
        base_url, 
        datastore=redis.StrictRedis()):
    channel_url = "{}/c/monitor".format(base_url)
    monitor_resp = requests.get(channel_url, stream=True)
    line_buffer = str()
    for char in monitor_resp.iter_content():
        line_buffer += char.decode()
        if line_buffer.endswith('\r\n'):
            line = line_buffer[0:-2]
            if line.startswith('null'):
                break
            message = json.loads(line)
            result = json.loads(message.get('value'))
            if 'location' in result:
                location = result.get('location')
                alert = result.get('event')
                in_ski_area = datastore.execute_command(
                    "GEORADIUS",
                    "colorado_ski_mountains",
                   location.get('longitude'),
                   location.get('latitude'),
                   100,
                   "km",
                   "WITHHASH")
                # Goes through each resort and add a weather alert
                # to a queue resort's hash value
                for row in in_ski_area:
                    queue_url = "{}/q/{}".format(base_url, row[1])
                    alert_result = requests.post(queue_url,
                        data={"value": alert})     
                    
                   
def weather_alert(
        base_url,
        geohash):
    channel_url = "{}/c/{}".format(base_url, geohash)
    ski_resort = requests.get(channel_url, stream=True)
    line_buffer = str()
    for char in ski_resort.iter_content():
        line_buffer += char.decode()
        if line_buffer.endswith("\r\n"):
            line = line_buffer[0:-2]
            if line.startswith('null')
                break
            message = json.load(line)
            print(message.get('value'))
