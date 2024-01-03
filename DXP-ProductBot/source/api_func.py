import requests
import os
import pymsteams
import math
import csv
import time
import pandas as pd
import concurrent.futures
from decouple import config
from datetime import datetime
from requests.exceptions import RequestException
from azure.storage.blob import BlobServiceClient

# Define your connection string and container and blob names
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(
    config("AZURE_STORAGE_CONNECTION_STRING")
)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(config("CONTAINER_NAME"))


def download_csv_file_from_blob(container_client, blob_name, local_data_dir, filename):
    if blob_name.endswith(".csv"):
        local_file_name = os.path.join(local_data_dir, filename)
        blob_client = container_client.get_blob_client(blob_name)
        try:
            with open(local_file_name, "wb") as data:
                blob_data = blob_client.download_blob()
                blob_data.readinto(data)
            print(f"===> Downloaded {blob_name} to {local_file_name}")
        except Exception as e:
            print(f"Error: {e}")
            current_time = datetime.now()
            yyyymmdd = current_time.strftime("%Y-%m-%d")
            send_notification(
                yyyymmdd,
                "0 rows",
                "File doesnt exist or ETL is still Running.",
                success=False,
            )
    else:
        print(f"===> Skipped {blob_name} as it is not a .csv file")


def read_csv_to_df(filename):
    try:
        df = pd.read_csv(filename, low_memory=False,escapechar="\\")
        df["UPC"] = df["UPC"].str.replace("'", "").astype(str)
        df["STORE_CODE"] = df["STORE_CODE"].astype(str)
        df["PRICE"] = df["PRICE"].astype(float)
        df["NAME"] = df["NAME"].astype(str)
        df["UNIT_PRICE"] = df["UNIT_PRICE"].astype(str)
        df["CATE_LEVEL1"] = df["CATE_LEVEL1"].astype(str)
        df["PRODUCT_SHORT_DESC"] = df["PRODUCT_SHORT_DESC"].astype(str)
        df["PRODUCT_IMG_URL"] = df["PRODUCT_IMG_URL"].astype(str)
        df = df[
            [
                "UPC",
                "STORE_CODE",
                "PRICE",
                "NAME",
                "UNIT_PRICE",
                "CATE_LEVEL1",
                "PRODUCT_SHORT_DESC",
                "PRODUCT_IMG_URL",
            ]
        ]
        return df
    except Exception as e:
        print("No data")


def upload_csv_to_blob(container_client, yyyymmdd, source_file_name):
    destination_file = f"{yyyymmdd}/{os.path.basename(source_file_name)}"
    blob_client = container_client.get_blob_client(destination_file)
    if not blob_client.exists():
        with open(source_file_name, "rb") as data:
            blob_client.upload_blob(data)
            print(
                f"===> Uploaded {source_file_name} to blob container {container_client}"
            )
    else:
        with open(source_file_name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(
                f"===> Overwritten {source_file_name} in blob container {container_client}"
            )

def clean_unit_price(unit_price):
    if '<br>' in unit_price:
        br_index = unit_price.find('<br>')
        cleaned_unit_price = unit_price[:br_index].strip()
    else:
        cleaned_unit_price = unit_price.strip()

    return cleaned_unit_price


def get_info_from_api_product(upc, store_code, url, headers, body):
    try:
        response = requests.post(url, headers=headers, json=body)
        # Check if the request was successful (status code 200)
        response.raise_for_status()
        data = response.json()
        product_info = []
        if "Products" in data and data["Products"]:
            sale_price_ecom = data["Products"][0]["SalePrice"]
            name = data["Products"][0]["Name"]
            desc = data["Products"][0]["ShortDescription"]
            image = data["Products"][0]["DefaultImage"]
            unit_price = data["Products"][0]["UnitPrice"]
            unit_price=clean_unit_price(unit_price)
            category = data["Products"][0]["Category"]
            price_ecom= data["Products"][0]["Price"]
            product_info.append(
                [sale_price_ecom, name, desc, image, unit_price, category,price_ecom]
            )
            return product_info
        elif "Items" in data and data["Items"]:
            sale_price_product = data["Items"][0]["SalePrice"]
            return sale_price_product
        else:
            return "None"
    except RequestException as e:
        return "None"


def compare_sale_price(price1, price2):
    return price1 == price2


def send_notification(yyyymmdd, upc, mess, success=True):
    myTeamsMessage = pymsteams.connectorcard(config("WEB_HOOK"))
    myTeamsMessage.title("Notification from DataOps-Bot")
    section = pymsteams.cardsection()
    if success:
        section.title("✅ Successful")
        section.activityTitle("Testing Details")
        section.addFact("Total UPC  :", f"{upc} ")
        section.addFact("Status", f"{mess}")
        section.addFact("Date", yyyymmdd)
    else:
        section.title("🔴 Errors")
        section.activityTitle("Testing Details")
        section.addFact("Total UPC  :", f"{upc} ")
        section.addFact("Error", f"{mess}")
        section.addFact("Date", yyyymmdd)
    myTeamsMessage.addSection(section)
    myTeamsMessage.text("Summary of the notification : Product Checker")
    myTeamsMessage.send()


def remove_file(directory, extension):
    for filename in os.listdir(directory):
        if filename.endswith(extension):
            file_path = os.path.join(directory, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
            else:
                print(f"No file file: {file_path}")
        else:
            continue

def check_info_df_and_save(df, output_csv_path):
    df['PRICE_CHECK'] = df.apply(lambda row: float(row['PRICE']) == float(row['PRICE_ECOM']), axis=1)
    df['NAME_CHECK'] = df.apply(lambda row: str(row['NAME']) in str(row['NAME_ECOM']), axis=1)
    df['DESC_CHECK'] = df.apply(lambda row: str(row['PRODUCT_SHORT_DESC']) == str(row['SHORT_DESCRIPTION_ECOM']), axis=1)
    df['IMAGE_CHECK'] = df.apply(lambda row: str(row['PRODUCT_IMG_URL']) == str(row['IMAGE_ECOM']), axis=1)
    df['UNIT_PRICE_CHECK'] = df.apply(lambda row: str(row['UNIT_PRICE']) == str(row['UNIT_PRICE_ECOM']), axis=1)
    df['CATEGORY_CHECK'] = df.apply(lambda row: str(row['CATE_LEVEL1']) == str(row['CATEGORY_ECOM']), axis=1)

    false_rows = df[df.apply(lambda row: any([not row[col] for col in df.columns[-6:]]), axis=1)]
    false_rows.to_csv(output_csv_path, index=False)