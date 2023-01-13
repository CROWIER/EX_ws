FROM python:3.8
ADD write_db.py .
ADD requirements.txt .
RUN pip install -r requirments.txt

CMD ["pythom", "./write_db.py"]