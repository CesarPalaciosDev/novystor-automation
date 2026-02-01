import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from models import auth_app, Product, Attributes
from datetime import datetime, timezone
from utils import *
import json
from time import sleep
from dotenv import load_dotenv
load_dotenv()


LOGS_PATH = os.getenv("LOGS_PATH")
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
ssl = os.getenv("ssl")
SECRET_KEY = os.getenv("SECRET_KEY")
MERCHANT_ID = os.getenv("MERCHANT_ID")
DAYS_TO_FETCH = os.getenv("DAYS")
CSV_FILE = f"{LOGS_PATH}/deliveries_log.csv"

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
logger.info('Retrieving data from db.')
with Session(engine) as session:
    last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))

if last_auth == None:
    logger.error("Failed authentication")
    sys.exit(0)
        
diff = datetime.now(timezone.utc) - last_auth.expire.replace(tzinfo=timezone.utc)
# The token expired
if diff.total_seconds()/3600 > 6:
    logger.warning('Refresh token expired.')
    sys.exit(0)

# Decrypt token
token = decrypt(last_auth.token, SECRET_KEY)
#print(f"Token: ${token}")
# Get products data
logger.info('Recolectando datos de atributos')
merchant_id = MERCHANT_ID
url = f"https://app.multivende.com/api/m/{merchant_id}/all-product-attributes"
headers = {
        'Authorization': f'Bearer {token}'
}
# Get data
response = requests.request("GET", url, headers=headers)
    
try:
    response = response.json()
    with open('data.json', 'w') as f:
        json.dump(response, f)
except Exception as e:
    logger.error(f"Hubo un error {e}: "+response.text)
    sys.exit(0)
# Obtenemos dos grupos de atributos, lo separamos
att = response["customAttributes"]
att_std = ["Season", "model", "description", "htmlDescription", "shortDescription",
            "htmlShortDescription", "Warranty", "Brand", "name", "ProductCategory", "sku_name", "color",
            "size", "sku", "internalSku", "width", "length", "height", "weight", "IDENTIFICADOR_PADRE", 
            "IDENTIFICADOR_HIJO", "tags", "picture url", "Codigo_de_barra_01", "Codigo_de_barra_02"]
# Transformamos los nombres para mayor comodidad
att_names = [item["name"]+"-"+item["CustomAttributeSet.name"] for item in att]
    
# Obtenemos la lista de todos los productos
logger.info("Solicitando ids de productos")
url = f"https://app.multivende.com/api/m/{merchant_id}/products/light/p/1"
response = requests.request("GET", url, headers=headers).json()
data = response["entries"]
pages = response["pagination"]["total_pages"]
# Los productos se organizan en paginas, pasamos por todas, guardando los resultados
for p in range(pages-1):
    url = f"https://app.multivende.com/api/m/{merchant_id}/products/light/p/{p+2}"
    response = requests.request("GET", url, headers=headers).json()
    data += response["entries"]

# Extraemos los id de cada uno
ids = [item["_id"] for item in data]

logger.info("Pagins totales: %d", pages)
logger.info("Total de productos: %d", len(ids))


sync_product_with_ids(ids, Product, engine)