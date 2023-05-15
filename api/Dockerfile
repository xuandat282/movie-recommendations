# syntax=docker/dockerfile:1.4
FROM python:3.7
WORKDIR /code
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .

ENTRYPOINT ["python"]
CMD ["-m", "flask", "--app", "app", "run", "--port=5000"]