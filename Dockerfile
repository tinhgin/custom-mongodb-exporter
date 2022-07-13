FROM python:3.9.13-slim

WORKDIR /app

EXPOSE 8890

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY main.py .

CMD python main.py
