__author__ = "Jeremy Nelson"

from celery import Celery

app =  Celery("room_reservation",
              broker="redis://localhost",
              backend="redis://localhost")
