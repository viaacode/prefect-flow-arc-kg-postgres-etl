FROM prefecthq/prefect:2-python3.9
COPY requirements.txt .
RUN pip install -r requirements.txt
ADD flows /opt/prefect/flows