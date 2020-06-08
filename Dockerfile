FROM python
RUN pip3 install boto3
RUN pip install python-csv
RUN pip install pandas
RUN pip install s3fs
RUN mkdir /src
COPY . /src 
CMD ["python", "/src/DdbWrite.py"]
