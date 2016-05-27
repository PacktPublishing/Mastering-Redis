__author__ = "Jeremy Nelson"

from celery import app

STATUS = ['Cancelled',
          'Confirmed', 
          'Denied',
          'Mediated',
          'Tentative']

@app.task
def availability(room):
    return STATUS[room.status] or None  

@app.task
def book(room):
    
    if not status in STATUS:
        raise ValueError("{} not a valid booking status".format(status)) 

@app.task
def reserve(room, start, duration):
    total = start + duration
    # Reserve room
    room.reserve = True
    available.delay(room, total)
    return "Room {} reserved. Starting at {} for duration = {}".format(start, total)

@app.task
def search(text):
    return text

@app.task
def room(name):
    return name

