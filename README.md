# Send Data To Business Application

## What does this function app do ?

Simply put, it takes the latest data from the data storage, merges it and formats it the correct way, sends the data to the business app through rabbit mq, and finally uploads it to the storage account in the history folder   
Here are the steps that this function app follows:

## Step 1: Send data to business app: send_data_to_queue()

This function is responsible for doing multiple things:
- It downloads the data from the "latest/" folder in the data storage
- It downloads the grid used in our business application 
- it concatenates all that data into one big dataframe
- sends the data on the "new_data_queue" which will be received on the business app

## Step 2: Pivot the data: pivot_df()

Before this, the sensor data is stored as valueType (Humidity, temperature etc...) and a value attached to it.   
When pivoting the data, we get for each time of measurement one measurement for each column.   
In other words, instead of having valueType and value as columns, we now have the valueTypes as columns directly

## Step 3: Uploading the data to storage: upload_new_data_to_blob_storage()

This function just divides the data by squareUUID and sends the data for each square in their own folder. Now we have folders for each square that contains all its measurement data in csv folders

## Step 4: Merging the history data: merge_history_data()

Here we just go through all the squares in our grid and perform the following operations:
- Merge all the csv files in that square folder into one big dataframe
- We group the data in a way where we have data for every 5 minutes interval
- delete all teh previous csv files
- upload the new csv file containing all the data formatted correctly
  
## Step 5: Deleting the latest/ folder: delete_folder()

Here we simply delete the "latest/" folder in the blob storage because the data in it has been formatted and sent to the history/ folder