"""Kanban Demonstration for Mastering Redis by Jeremy Nelson

Licensed under the GPLv3
"""
__author__ = "Jeremy Nelson"

import redis
import sys

class BuildBoxWorkstation(object):

    def __init__(self, 
                 database = redis.StrictRedis()):
        self.database = database
        self.messages = self.database.pubsub()
        self.messages.subscribe("kanban")
        self.messages.subscribe("operations")
        self.input_bin, self.output_bin = 0, []
        self.threshold = 5

    def __construct_box__(self):
        if self.input_bin > 0:
            self.input_bin -= 1
        if self.input_bin <= self.threshold:
            self.__pull_material__()
        # Cuts and assembles box with a lid
        self.output_bin = [i for i in range(1, self.input_bin)]
        # Sends Kanban Message to next station
        message = "READY Box {}".format(len(self.output_bin))
        self.database.publish(
            "kanban", 
            message)
        print(message)

    def __pull_material__(self):
        # Sends message to supplier to order wood
        message = "ORDER wood"
        self.database.publish("kanban", message)
        print("{}, input_bin={}".format(message, self.input_bin))
        # For convenience we'll just add an order of 5
        # to our input bin in a real operation, an order
        # would be placed, hopefully with a kind supplier API
        self.input_bin += 5

    def run(self):
        while 1:
            for item in self.messages.listen():
                channel = item.get('channel')
                message = item.get('data')
                type_of = item.get('type')
                if type(message) == int:
                    continue
                message = message.decode()
                if channel == b"kanban":
                    if message.startswith("PULL Box"):
                        if len(self.output_bin) > 0:
                            message = "READY Box {}".format(len(self.output_bin))
                            self.database.publish(
                                "kanban",
                                message)
                            print(message)
                        else:
                            self.__construct_box__()
                        # Output bin is less one
                        self.output_bin.pop()
                if channel == b"operations":
                    if message.startswith("STOP"):
                        print("Stopping")
                        sys.exit(1)


if  __name__ == '__main__':
    box_workstation = BuildBoxWorkstation()
    print("Running BuildBoxWorstation")
    box_workstation.run()

    
    
