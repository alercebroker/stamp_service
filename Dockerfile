FROM python:3.6.8

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt && pip install flask gunicorn

COPY . /app
EXPOSE 8087
ENV PYTHONHASHSEED=0

WORKDIR /app/scripts
CMD ["gunicorn", "-b", "0.0.0.0:8087", "-w", "6", "wsgi"]
