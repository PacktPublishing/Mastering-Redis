FROM python:3.4.3
RUN pip3 install flask redis
COPY . /info_app
WORKDIR /info_app
EXPOSE 5001
CMD ["python", "info.py"]
