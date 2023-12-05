import logging
import azure.functions as func
import pika
import os
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd

app = func.FunctionApp()

@app.schedule(schedule="* */15 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def sendDataToBA(myTimer: func.TimerRequest) -> None:
    send_data_to_queue()
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

def send_data_to_queue():
    connection_params = pika.ConnectionParameters(
        host='localhost',
        port=5673,
        virtual_host='/',
        credentials=pika.PlainCredentials('myuser', 'mypassword')
    )

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    queue_name = 'new_data_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    
    connection_string = os.environ["MyStorageAccountConnection"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("csv")
    blobs = container_client.list_blobs()
    for b in blobs:
        print(b.name)
        download_blob_to_file(blob_service_client, "csv", b.name)

    channel.basic_publish(exchange='', routing_key=queue_name, body="hello")
    logging.info(f"sent hello to queue {queue_name}")

    connection.close()

def download_blob_to_file(blob_service_client: BlobServiceClient, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    csv_file = StringIO(blob_data.readall().decode('utf-8'))
    df = pd.read_csv(csv_file)
    return df
 