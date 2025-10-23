from utils import *
from dotenv import load_dotenv
load_dotenv()


SECRET_KEY = os.getenv("SECRET_KEY")

res = webhook_load_checkout("e8801cf3-08d0-4bcc-9c52-ad53646f0d71")

print("Esta es la venta", res)
