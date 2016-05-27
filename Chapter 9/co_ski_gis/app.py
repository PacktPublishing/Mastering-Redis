__author__ = "Jeremy Nelson"

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
                    
                   
                
   
