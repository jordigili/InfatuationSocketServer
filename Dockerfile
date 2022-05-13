# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 9090 9099
CMD [ "./wait_server_ip.sh"]
CMD ["python3", "main.py"]