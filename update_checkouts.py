import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
import time
from datetime import datetime, timedelta
from models import auth_app, checkouts
import pandas as pd
import numpy as np
from utils import *
from dotenv import load_dotenv
load_dotenv()

LOGS_PATH = os.getenv("LOGS_PATH")
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
ssl = os.getenv("ssl")
SECRET_KEY = os.getenv("SECRET_KEY")
MERCHANT_ID = os.getenv("MERCHANT_ID")
DAYS_TO_FETCH = os.getenv("DAYS") 
CSV_FILE = f"{LOGS_PATH}/checkouts_log.csv"

writeCsvLog(CSV_FILE, "INFO", "Job started", "Update checkouts job has succesfully started")

st = time.time()
# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

# Making engine
engine = create_engine(SQLALCHEMY_DATABASE_URI,
                        pool_recycle=3600,   # recycle connections every hour
                        pool_pre_ping=True,
                        connect_args={
                            "ssl_ca": ssl
                            }
                        )

# Get data from tables
writeCsvLog(CSV_FILE, "INFO", "DB Initializing", "The db session is initializing")
logger.info('Retrieving data from db.')
with Session(engine) as session:
    last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))
    result = session.scalar(select(checkouts).order_by(checkouts.fecha.desc()))
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    last_update = datetime.now() - timedelta(days=int(DAYS_TO_FETCH)) # One day before to update changes of recents sells
    last = last_update.strftime("%Y-%m-%dT%H:%M:%S")
writeCsvLog(CSV_FILE, "INFO", "DB Initialized", "The db session has been initialized")

if last_auth == None:
    logger.error("Failed authentication")
    writeCsvLog(CSV_FILE, "ERROR", "Failed authentication", "Please review the auth data")
    sys.exit(0)
        
diff = datetime.now() - last_auth.expire
# The token expired
if diff.total_seconds()/3600 > 6:
    logger.warning('Refresh token expired.')
    writeCsvLog(CSV_FILE, "WARNING", "Refresh token expired", "Please review the refresh token an try again")
    sys.exit(0)

# Decrypt token
token = decrypt(last_auth.token, SECRET_KEY)

# Get checkouts data
logger.info('Recolectando datos de ventas')
writeCsvLog(CSV_FILE, "INFO", "Getting checkouts", "Calling the Multivende API to get the checkouts")
merchant_id = MERCHANT_ID
url = f"https://app.multivende.com/api/m/{merchant_id}/checkouts/light/p/1?_sold_at_from={last}&_sold_at_to={now}"
headers = {
        'Authorization': f'Bearer {token}'
}

print(url)
# Get id data from the checkouts
response = requests.request("GET", url, headers=headers)
try:
    response = response.json()
except Exception as e:
    logger.error(f'Hubo un error {e}: {response.text}')

print(response)
    
pages = response["pagination"]["total_pages"]
ids= []
# Extract all ids
logger.info('Cargando ids de ventas.')
for p in range(0, pages):
    url = f"https://app.multivende.com/api/m/{merchant_id}/checkouts/light/p/{p+1}?_updated_at_from={last}&_updated_at_to={now}"
    data = requests.get(url, headers=headers)
    try:
        data = data.json()
    except Exception as e:
        logger.error(f'Hubo un error {e}: {response.text}')
    
    for d in data["entries"]:
        ids.append(d["_id"])

# Now the information completed
logger.info('Cargando informacion de ventas.')
writeCsvLog(CSV_FILE, "INFO", "Total checkouts", f"Total checkouts retrieved in API call {len(ids)}")
logger.info(f"Total checkouts retrieved in API call {len(ids)}")

ventas = []
count = 0

#print("Id totales: ", len(ids))

for id in ids:
    tmp = {}
    url = f"https://app.multivende.com/api/checkouts/{id}"
    checkout = requests.get(url, headers=headers)
    try:
        checkout = checkout.json()
        checkout['soldAt']
        count = count + 1
        #print("Checkout agregado, cuenta: ", count)
        #print(checkout)
        #print("\n\n")
    except Exception as e:
        logger.error(f"Error {e}: {checkout}")
        
    tmp["fecha"] = checkout["soldAt"]
    tmp["nombre"] = checkout["Client"]["fullName"]
    tmp["n venta"] = checkout["CheckoutLink"]["externalOrderNumber"] # Numero de orden en marketplace
    tmp["id"] = checkout["CheckoutLink"]["CheckoutId"] # Codigo en multivende
    tmp["estado entrega"] = checkout["deliveryStatus"]
    tmp["costo de envio"] = checkout["DeliveryOrderInCheckouts"][0]["DeliveryOrder"]["cost"]
    tmp["market"] = checkout["origin"]
    tmp["mail"] = checkout["Client"]["email"]
    tmp["phone"] = checkout["Client"]["phoneNumber"]
    # Try to find the billing files
    try:
        url = f"https://app.multivende.com/api/checkouts/{id}/electronic-billing-documents/p/1"
        billing = requests.get(url, headers=headers).json()
        tmp["estado boleta"] = billing["entries"][-1]["ElectronicBillingDocumentFiles"][-1]["synchronizationStatus"]
        tmp["url boleta"] = billing["entries"][-1]["ElectronicBillingDocumentFiles"][-1]["url"]
    except:
        tmp["estado boleta"] = None
        tmp["url boleta"] = None
        
    # Getting all status of ventas
    tmp["estado venta"] = []
    for status in checkout["CheckoutPayments"]:
        tmp["estado venta"].append(status["paymentStatus"])
    # For each item we split the checkout
    for product in checkout["CheckoutItems"]:
        item = tmp.copy()
        item["codigo producto"] = product["code"]
        item["nombre producto"] = product["ProductVersion"]["Product"]["name"]
        item["id padre producto"] = product["ProductVersion"]["ProductId"]
        item["id hijo producto"] = product["ProductVersionId"]
        item["cantidad"] = product["count"]
        item["precio"] = product["totalWithDiscount"]
        ventas.append(item)

# Load data to be processed
logger.info('Limpiando los datos.')
df = pd.DataFrame(ventas)
df["fecha"] = pd.to_datetime(df["fecha"])
df["fecha"] = df["fecha"].dt.tz_convert(None)
df= df.fillna(np.nan)
for i in df["estado venta"].index:
    df.loc[i, "estado venta"] = df["estado venta"][i][-1]
    
df = df.replace({np.NaN: None})

logger.info('Cargando a la DB.')
writeCsvLog(CSV_FILE, "INFO", "Loading data", "Loading deliveries data into the db")
check_difference_and_update_checkouts(CSV_FILE,df, checkouts, engine)
et = time.time()
elapsed_time = et - st
writeCsvLog(CSV_FILE, "INFO", "Job succeded",  f"This job has been completed succesfully in {elapsed_time} seconds")
logger.info(f"This job has been completed succesfully in {elapsed_time} seconds")
