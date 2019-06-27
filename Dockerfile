FROM python:3.6.8
ADD . .
RUN pip install -r requirements.txt
RUN pip install flask gunicorn
EXPOSE 8087
ENV PYTHONHASHSEED=0
CMD ["gunicorn", "-b", "0.0.0.0:8087", "-w", "4", "simple_server"]
