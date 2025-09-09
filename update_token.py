import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from models import auth_app
from utils import *
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

LOGS_PATH = os.getenv("LOGS_PATH")
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
ssl = os.getenv("ssl")
SECRET_KEY = os.getenv("SECRET_KEY")
MERCHANT_ID = os.getenv("MERCHANT_ID")
DAYS_TO_FETCH = os.getenv("DAYS") 
CSV_FILE = f"{LOGS_PATH}/checkouts_log.csv"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

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

logger.info("Updating token")
url = "https://app.multivende.com/oauth/access-token"

# Get last token
with Session(engine) as session:
    last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))
    
# Check if exists token
if last_auth == None:
    logger.info("No hay token disponible")
    sys.exit(0)

refresh_token = last_auth.refresh_token

# Prepare data
payload = json.dumps({
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,    
    "grant_type": "refresh_token",
    "refresh_token": refresh_token}
)
    
headers = {
    'cache-control': 'no-cache',
    'Content-Type': 'application/json'
}
logger.info("Realizando solicitud.")
response = requests.post(url, headers=headers, data=payload)

try:
    # Guardamos la informacion requerida y logueamos
    token = response.json()["token"]
    expiresAt = response.json()["expiresAt"]
    refresh_token = response.json()["refreshToken"]
    encrypted = encrypt(token, SECRET_KEY)
    authentication = auth_app(token = encrypted, expire=datetime.fromisoformat(expiresAt), refresh_token=refresh_token)
    with Session(engine) as session:
        session.add(authentication)
        session.commit()
    logger.info("Actulizacion exitosa.")
except:
    logger.error(f"Hubo un error con la actualizacion: {response.text}")
