FROM python:2.7
COPY *.py ./
COPY requirements ./
RUN pip install -r requirements
EXPOSE 5000
CMD python api.py
