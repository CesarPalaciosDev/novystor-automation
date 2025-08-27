#Script updated sucessfully 10/04/2025

import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from models import auth_app, deliverys, checkouts
import pandas as pd
import numpy as np
import json
from utils import *
from dotenv import load_dotenv
load_dotenv()

LOGS_PATH = os.getenv("LOGS_PATH")
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
ssl = os.getenv("ssl")
SECRET_KEY = os.getenv("SECRET_KEY")
MERCHANT_ID = os.getenv("MERCHANT_ID")
DAYS_TO_FETCH = os.getenv("DAYS")
CSV_FILE = f"{LOGS_PATH}/deliveries_log.csv"

#Log job initialization
writeCsvLog(CSV_FILE, "INFO", "Job started", "Update deliveries job has succesfully started")

st = time.time()

# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

# Making engine
engine = create_engine(SQLALCHEMY_DATABASE_URI,
                       connect_args={
                            "ssl": {
                                "ca":ssl
                                }     
                            }
                       )

# Get data from tables
logger.info('Retrieving data from db.')
writeCsvLog(CSV_FILE, "INFO", "DB Initializing", "The db session is initializing")
with Session(engine) as session:
    last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))
    last_date = datetime.now() - timedelta(days=int(20))
    result = session.scalars(select(checkouts.id_venta).where(checkouts.fecha >= last_date)).all()
writeCsvLog(CSV_FILE, "INFO", "DB Initialized", "The db session has been initialized")

if last_auth == None:
    logger.error("Failed authentication")
    writeCsvLog(CSV_FILE, "ERROR", "Failed authentication", "Please review the auth data")
    sys.exit(0)
        
diff = datetime.now() - last_auth.expire
# The token expired
if diff.total_seconds()/3600 > 6:
    writeCsvLog(CSV_FILE, "WARNING", "Refresh token expired", "Please review the refresh token an try again")
    logger.warning('Refresh token expired.')
    sys.exit(0)

# Decrypt token
token = decrypt(last_auth.token, SECRET_KEY)

# Get marketplace connections
logger.info('Getting of deliveries from checkouts')
data = [] 

# with open('data.json', 'w') as f:
#      json.dump(result, f)
merchant_id = MERCHANT_ID

#Write total rows read to csv log
writeCsvLog(CSV_FILE, "INFO", "Getting deliveries from checkouts", f"Total rows read {len(result)}")
logger.info(f"Total rows read {len(result)}")

#print(len(result))
for id in result:
    url = f"https://app.multivende.com/api/checkouts/{id}"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
        #print(response)
        response['DeliveryOrderInCheckouts']  # Check if the json data is correct
    except Exception as e:
        logger.error(f'Error {e}: {response.text}')

    if response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['trackingNumber'] is None:
        continue

    tmp = {}
    tmp['n venta'] = response["CheckoutLink"]["externalOrderNumber"] 
    tmp['fecha promesa'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['promisedDeliveryDate']
    tmp['direccion'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['deliveryAddress']
    tmp['codigo'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['code']
    tmp['courier'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['courierName']
    tmp['clase de envio'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingMode']
    tmp['fecha despacho'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['handlingDateLimit']
    tmp['delivery status'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['deliveryStatus']
    n_seguimiento = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['trackingNumber'] 
    if len(n_seguimiento) == 21:
        tmp['N seguimiento'] = n_seguimiento[3:-7]
    tmp['status etiqueta'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingLabelStatus']
    tmp['estado impresion etiqueta'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingLabelPrintStatus']
    tmp['id venta'] = response['_id']
    tmp['codigo venta'] = response['code']
    data.append(tmp)

# Create dataframe and adjust formats
df = pd.DataFrame(data)
#df.to_csv('temp_init.csv')
df.fillna(np.nan, inplace=True)

df["fecha despacho"] = pd.to_datetime(df["fecha despacho"])
df["fecha despacho"] = df["fecha despacho"].dt.tz_convert(None)
df["fecha promesa"] = pd.to_datetime(df["fecha promesa"])
df["fecha promesa"] = df["fecha promesa"].dt.tz_convert(None)

# Fill empty values with None
df = df.replace({np.NaN: None})

# Clear duplicated
df = df.drop_duplicates()

# Only store the items with n venta (a checkout registered)
df = df[df["n venta"].notna()]

# Fill empty couriers
df['courier'].fillna('Empty', inplace=True)

#Fill empty shipping modes
df['clase de envio'].fillna('Empty', inplace=True)
df['clase de envio'].replace('', 'Empty', inplace=True)

# Only store the items with N seguimiento and fecha despacho
df = df[df["N seguimiento"].notna()]
df = df[df["fecha despacho"].notna()]

# Check the data and load to database
logger.info('Cargando a la base de datos')
writeCsvLog(CSV_FILE, "INFO", "Loading data", "Loading deliveries data into the db")


check_diferences_and_update_deliverys(CSV_FILE,df, deliverys, engine)
##df.to_csv('temp_deliveries.csv')

writeCsvLog(CSV_FILE, "INFO", "Data loaded", "The data has succesfully load into the db")

et = time.time()
elapsed_time = et - st

logger.info('Trabajo completado.')
writeCsvLog(CSV_FILE, "INFO", "Job succeded",  f"This job has been completed succesfully in {elapsed_time} seconds")
logger.info(f"This job has been completed succesfully in {elapsed_time} seconds")