import concurrent.futures
import csv
import math
import os
import time
from datetime import datetime

import pandas as pd
from api_func import *
from api_func import (
    check_info_df_and_save,
    clean_unit_price,
    download_csv_file_from_blob,
    get_info_from_api_product,
    read_csv_to_df,
    remove_file,
    send_notification,
    upload_csv_to_blob,
)
from decouple import config


def process_row(row):
    upc = row["UPC"]
    store_code = row["STORE_CODE"]
    regular_price = row["PRICE"]
    name = row["NAME"]
    image = row["PRODUCT_IMG_URL"]
    desc = row["PRODUCT_SHORT_DESC"]
    category = row["CATE_LEVEL1"]
    unit_price = row["UNIT_PRICE"]
    unit_price = clean_unit_price(unit_price)

    url_ecom = config("URL_ECOM")
    headers_ecom = {"content-type": "application/json; charset=UTF-8"}
    body_ecom = {
        "Keyword": upc,
        "StoreID": store_code,
        "PageSize": 1,
        "PageNum": 1,
    }

    product_info = get_info_from_api_product(
        upc, store_code, url_ecom, headers_ecom, body_ecom
    )

    # get infos from API RSECOM
    price_ecom = product_info[0][6]
    row["PRICE_ECOM"] = price_ecom
    name_ecom = product_info[0][1]
    row["NAME_ECOM"] = name_ecom
    desc_ecom = product_info[0][2]
    row["SHORT_DESCRIPTION_ECOM"] = desc_ecom
    image_ecom = product_info[0][3]
    row["IMAGE_ECOM"] = image_ecom
    unit_price_ecom = product_info[0][4]
    row["UNIT_PRICE_ECOM"] = unit_price_ecom
    category_ecom = product_info[0][5]
    row["CATEGORY_ECOM"] = category_ecom

    with open("data/regular_info.csv", mode="a", newline="") as file:
        fieldnames = [
            "UPC",
            "STORE_CODE",
            "PRICE",
            "NAME",
            "UNIT_PRICE",
            "CATE_LEVEL1",
            "PRODUCT_SHORT_DESC",
            "PRODUCT_IMG_URL",
            "PRICE_ECOM",
            "NAME_ECOM",
            "UNIT_PRICE_ECOM",
            "CATEGORY_ECOM",
            "SHORT_DESCRIPTION_ECOM",
            "IMAGE_ECOM",
        ]

        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if os.stat("data/regular_info.csv").st_size == 0:
            writer.writeheader()

        writer.writerow(
            {
                "UPC": "'" + upc,
                "STORE_CODE": str(store_code),
                "PRICE": float(regular_price),
                "NAME": str(name),
                "UNIT_PRICE": str(unit_price),
                "CATE_LEVEL1": str(category),
                "PRODUCT_SHORT_DESC": str(desc),
                "PRODUCT_IMG_URL": str(image),
                "PRICE_ECOM": float(price_ecom),
                "NAME_ECOM": str(name_ecom),
                "UNIT_PRICE_ECOM": str(unit_price_ecom),
                "CATEGORY_ECOM": str(category_ecom),
                "SHORT_DESCRIPTION_ECOM": str(desc_ecom),
                "IMAGE_ECOM": str(image_ecom),
            }
        )


def process_batch(df_batch):
    for _, row in df_batch.iterrows():
        process_row(row)


def process_regular_product(yyyymmdd):
    print("CHECKING INFO OF PRODUCT WITH REGULAR PRICE")
    current_time = datetime.now()

    starttime = time.time()
    local_data_dir = config("LOCAL_DIR")
    blob_name = f"{yyyymmdd}/price_regular_of_product.csv"
    download_csv_file_from_blob(
        CONTAINER_CLIENT, blob_name, local_data_dir, "price_regular_of_product.csv"
    )
    filename = "data/price_regular_of_product.csv"
    df = read_csv_to_df(filename)
    total_rows = df.shape[0]
    print(f"===> Num of rows : {total_rows} rows")

    print("===> Generate info of product by upc: ")
    chunks = list()
    batch_num = int(config("BATCH_NUM"))
    batch_size = math.ceil((len(df) / batch_num))
    for i in range(batch_num):
        chunks.append(df[i * batch_size : (i + 1) * batch_size])
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(process_batch, chunks)
    print(f"===> Executed in {time.time() - starttime} seconds.")
    print("===> Generate csv successfull ")

    upload_csv_to_blob(CONTAINER_CLIENT, yyyymmdd, f"data/regular_info.csv")

    df = pd.read_csv("data/regular_info.csv")
    df["PRICE"] = df["PRICE"].apply(lambda x: f"{x:.2f}")
    df["PRICE_ECOM"] = df["PRICE_ECOM"].apply(lambda x: f"{x:.2f}")
    df.to_csv("data/regular_info.csv", index=False)

    output = f"data/price_regular_product_check_{yyyymmdd}.csv"
    error_rows = check_info_df_and_save(df, output)
    upload_csv_to_blob(CONTAINER_CLIENT, yyyymmdd, output)

    error_rows = pd.read_csv(f"data/price_regular_product_check_{yyyymmdd}.csv")
    check = error_rows.shape[0]
    if check == 0:
        mess = "Everything is Ok!"
        print(f"===> {mess}")
        yyyymmdd = current_time.strftime("%Y-%m-%d")
        send_notification(yyyymmdd, total_rows, mess, success=True)
    else:
        print("===> Product error")
        print(f"===> Num of errors : {check}")
        link = (
            config("LINK") + f"/{yyyymmdd}/price_regular_product_check_{yyyymmdd}.csv"
        )
        yyyymmdd = current_time.strftime("%Y-%m-%d")
        send_notification(yyyymmdd, total_rows, link, success=False)

    remove_file(local_data_dir, "csv")
    print(f"===> Processing finished in {time.time() - starttime} seconds.")

    return {
        "status": "Success" if check == 0 else "Fail",
        "total upc": total_rows,
        "message": mess if check == 0 else None,
        "Num of errors": f"{check}" if check != 0 else None,
        "link URL": link if check != 0 else None,
    }
