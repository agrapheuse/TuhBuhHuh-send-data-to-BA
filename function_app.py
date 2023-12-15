import logging
import azure.functions as func
import pika
import os
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
import json
import uuid
from enum import Enum
from datetime import datetime

app = func.FunctionApp()

# event header class
class EventCatalog(Enum):
    NEW_SENSOR_DATA = 'NEW_SENSOR_DATA'

class EventHeader:
    def __init__(self, eventID, eventCatalog):
        self.eventID = eventID
        self.eventCatalog = eventCatalog


# function triggers on a timer every 15 minutes
@app.function_name(name="sendDataToBA")
@app.schedule(schedule="* */5 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def sendDataToBA(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function executed.')
    connection_string = "DefaultEndpointsProtocol=https;AccountName=datalaketuhbehhuh;AccountKey=C2te9RgBRHhIH8u3tydAsn9wNd4umdD2axq1ZdcfKh7CZRpL04+D4H6QinE/gckMTUA/dFj1kFpd+ASt4+/8ZA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    df = send_data_to_queue(blob_service_client)
    if df.empty:
        return
    pivoted_df = pivot_df(df)
    upload_aggregated_data(blob_service_client, pivoted_df)
    merge_aggregated_data(blob_service_client)
    delete_latest_folder()

def send_data_to_queue(blob_service_client):
    # connect to rabbitmq using the right credentials
    connection_params = pika.ConnectionParameters(
        host='localhost',
        port=5673,
        virtual_host='/',
        credentials=pika.PlainCredentials('myuser', 'mypassword')
    )

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    # declare the queue
    queue_name = 'new_data_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    
    # get all the blobs names in the csv container
    container_client = blob_service_client.get_container_client("csv")
    blobs = container_client.list_blobs(name_starts_with="latest/")
    
    # get the grid from the blob storage
    grid = download_blob_to_file(blob_service_client, "grid", "grid.csv")

    # for every blob, download it to a dataframe and add it to a list
    new_data = []
    for b in blobs:
        print(b.name)
        df = download_blob_to_file(blob_service_client, "csv", b.name)
        new_data.append(df)
        logging.info(f"downloaded {b.name}")
    
    # concatenate all the dataframes into one big dataframe
    concatenated_data = concatenate_new_data(grid, new_data)

    # convert the dataframe to dict
    data_list = concatenated_data.to_dict(orient='records')

    # create the event message
    event_header = EventHeader(uuid.uuid4(), EventCatalog.NEW_SENSOR_DATA)
    event_message_dict = {
        "eventHeader": {
            "eventID": str(event_header.eventID),
            "eventCatalog": event_header.eventCatalog.value
        },
        "eventBody": f"{json.dumps(data_list)}"
    }
    # convert the event message to json
    json_payload = json.dumps(event_message_dict)

    # send the event message to the queue
    channel.basic_publish(exchange='', routing_key=queue_name, body=json_payload)
    logging.info(f"sent new data to queue {queue_name}")

    # close the connection
    connection.close()
    return concatenated_data

def download_blob_to_file(blob_service_client: BlobServiceClient, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()

    csv_file = StringIO(blob_data.readall().decode('utf-8'))
    df = pd.read_csv(csv_file)
    return df

def concatenate_new_data(grid, new_data):
    # create a dataframe with the right columns
    columns = ["squareUUID", "timestamp", "valueType", "value"]
    concatenated_data = pd.DataFrame(columns=columns)

    # loop through all the squares in the grid
    for index, grid_row in grid.iterrows():
        # loop through all the dataframes
        for df in new_data:
            df.replace("P1", "PM10", inplace=True)
            df.replace("P2", "PM25", inplace=True)

            # filter all the rows that contain data that are in the square
            data_in_square = df[
                (df["latitude"] < grid_row["top_left_lat"]) & 
                (df["latitude"] > grid_row["bottom_right_lat"]) & 
                (df["longitude"] > grid_row["top_left_long"]) & 
                (df["longitude"] < grid_row["bottom_right_long"])
                ]
            
            # add those rows to the new dataframe
            for index, data_row in data_in_square.iterrows():
                concatenated_data.loc[len(concatenated_data)] = {
                    "squareUUID": grid_row["uuid"],
                    "timestamp": data_row["timestamp"],
                    "valueType": data_row["valueType"],
                    "value": data_row["sensorDataValue"]
                }
    return concatenated_data

def pivot_df(df):
    pivoted_df = df.pivot_table(index=["squareUUID", "timestamp"], columns="valueType", values="value", aggfunc="first")
    pivoted_df.reset_index(inplace=True)
    return pivoted_df

def upload_aggregated_data(blob_service_client, df):
    squares = df["squareUUID"].unique()
    for square in squares:
        square_df = df[df["squareUUID"] == square]
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        csv_string = square_df.to_csv(index=False)
        blob_client = blob_service_client.get_blob_client(container="csv/history", blob=f"{square}/{time}.csv")

        blob_client.upload_blob(csv_string, blob_type="BlockBlob")
        logging.info(f"uploaded {square}/{time}.csv to blob storage")

def delete_latest_folder():
    connection_string = os.environ["MyStorageAccountConnection"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("csv")

    try:
        # List blobs with the specified prefix
        blobs = container_client.walk_blobs(name_starts_with="latest/")

        # Delete each blob
        for blob in blobs:
            container_client.get_blob_client(blob.name).delete_blob()

        print(f"Folder 'latest/' deleted successfully.")
    except Exception as e:
        print(f"Error deleting folder 'latest/': {str(e)}")

def merge_aggregated_data(blob_service_client):
    folder_list = list_folders(blob_service_client, "csv")
    print(folder_list)
    for folder in folder_list:
        merged_data_df = merge_data(blob_service_client, folder)
        reformatted_time_df = merge_on_time(merged_data_df)
        delete_folder(blob_service_client, folder)
        upload_merged_data(blob_service_client, reformatted_time_df, folder)

def list_folders(blob_service_client, container_name, folder_name="history/"):
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=folder_name)

    folder_set = set()

    for blob in blob_list:
        blob_name = blob.name
        # Extract folder name
        folder_name = "/".join(blob_name.split('/')[:-1])
        folder_set.add(folder_name)

    return folder_set

def merge_data(blob_service_client, folder):
    container_client = blob_service_client.get_container_client("csv")
    blob_list = container_client.list_blobs(name_starts_with=folder)
    merged_df = pd.DataFrame(columns=["squareUUID", "timestamp", "BIKE", "CAR", "HEAVY", "HUMIDITY", "PEDESTRIAN", "PM10", "PM25", "TEMPERATURE"])
    for blob in blob_list:
        df = download_blob_to_file(blob_service_client, "csv", blob.name)
        merged_df = merge_two_df(merged_df, df)
    merged_df = merged_df.drop(columns="squareUUID", axis=1)
    return merged_df

def merge_two_df(df1, df2):
    appended_df = pd.concat([df1, df2], ignore_index=True)
    return appended_df

def merge_on_time(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    # Resample the data to 5-minute intervals and calculate the mean
    resampled_data = df.resample('5T').mean()

    # Reset the index to make 'timestamp' a regular column again
    resampled_data.reset_index(inplace=True)
    print(resampled_data.columns)
    for col in resampled_data.columns[1:]:
        if resampled_data[col].dtype == 'float64':
            resampled_data[col] = resampled_data[col].round(1)
    return resampled_data

def delete_folder(blob_service_client, folder):
    container_client = blob_service_client.get_container_client("csv")
    blob_list = container_client.list_blobs(name_starts_with=folder)
    for blob in blob_list:
        container_client.get_blob_client(blob.name).delete_blob()
    print(f"Folder '{folder}' deleted successfully.")

def upload_merged_data(blob_service_client, df, folder):
    container_client = blob_service_client.get_container_client("csv")
    csv_string = df.to_csv(index=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    blob_client = blob_service_client.get_blob_client(container="csv", blob=f"{folder}/{now}.csv")
    blob_client.upload_blob(csv_string, blob_type="BlockBlob")
    logging.info(f"uploaded {folder}.csv to blob storage")