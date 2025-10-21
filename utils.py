from sqlalchemy.orm import Session
from sqlalchemy import select, update, create_engine
from models import auth_app, checkouts_full, checkout_items
from cryptography.fernet import Fernet
import pandas as pd
import numpy as np
import logging
import sys
import requests
import pytz
import csv
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

LOGS_PATH = os.getenv("LOGS_PATH")
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
ssl = os.getenv("ssl")
SECRET_KEY = os.getenv("SECRET_KEY")
MERCHANT_ID = os.getenv("MERCHANT_ID")
DAYS_TO_FETCH = os.getenv("DAYS") 
CSV_FILE = f"/checkouts_log.csv"


# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

def webhook_load_checkout(id):
    engine = create_engine(SQLALCHEMY_DATABASE_URI,
                        pool_recycle=3600,   # recycle connections every hour
                        pool_pre_ping=True,
                        connect_args={
                            "ssl_ca": ssl
                            }
                        )
    with Session(engine) as session:
        last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))
    
    #rint(last_auth.expire)

    if last_auth == None:
        logger.error("Failed authentication")
        sys.exit(0)
    
    diff = datetime.now() - last_auth.expire
    # The token expired
    if diff.total_seconds()/3600 > 6:
        logger.warning('Refresh token expired.')
        sys.exit(0)

    # Decrypt token
    token = decrypt(last_auth.token, SECRET_KEY)

    headers = {
        'Authorization': f'Bearer {token}'
    }

    ventas = []
    productos = []

    tmp = {}
    url = f"https://app.multivende.com/api/checkouts/{id}"
    checkout = requests.get(url, headers=headers)
    try:
        checkout = checkout.json()
        checkout['soldAt']
        #count = count + 1
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
    # Campos agregados
    if checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['promisedDeliveryDate'] == None:
        tmp['fecha promesa'] = '2262-04-11 23:47:16.854775Z'
    else:
        tmp['fecha promesa'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['promisedDeliveryDate']
    try:
        tmp['direccion'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['deliveryAddress'][0:79]
    except (TypeError, KeyError, IndexError):
        tmp['direccion'] = None  # or some default value like ""
    tmp['codigo'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['code']
    tmp['courier'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['courierName']
    tmp['clase de envio'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingMode']
    tmp['fecha despacho'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['handlingDateLimit']
    tmp['delivery status'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['deliveryStatus']
    n_seguimiento = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['trackingNumber'] 
    if n_seguimiento and len(n_seguimiento) == 21:
        tmp['N seguimiento'] = n_seguimiento[3:-7]
    tmp['status etiqueta'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingLabelStatus']
    tmp['estado impresion etiqueta'] = checkout['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingLabelPrintStatus']
    tmp['id venta'] = checkout['_id']
    tmp['codigo venta'] = checkout['code']

    ventas.append(tmp)
    
    # For each item we split the checkout
    for product in checkout["CheckoutItems"]:
        item = {
        "codigo producto": product["code"],
        "nombre producto": product["ProductVersion"]["Product"]["name"],
        "id padre producto": product["ProductVersion"]["ProductId"],
        "id hijo producto": product["ProductVersionId"],
        "cantidad": product["count"],
        "precio": product["totalWithDiscount"],
        "id venta": checkout["CheckoutLink"]["CheckoutId"]
        }
        productos.append(item)

    #Crear dataframes
    print(ventas)

    df = pd.DataFrame(ventas)
    dfp = pd.DataFrame(productos)

    df["fecha"] = pd.to_datetime(df["fecha"])
    df["fecha"] = df["fecha"].dt.tz_convert(None)

    df.fillna(np.nan, inplace=True)

    # Fill empty couriers
    df['courier'].fillna('Empty', inplace=True)
    df["fecha despacho"] = pd.to_datetime(df["fecha despacho"])
    df["fecha despacho"] = df["fecha despacho"].dt.tz_convert(None)
    df["fecha promesa"] = pd.to_datetime(df["fecha promesa"])
    df["fecha promesa"] = df["fecha promesa"].dt.tz_convert(None)
    df= df.fillna(np.nan)
    for i in df["estado venta"].index:
        df.loc[i, "estado venta"] = df["estado venta"][i][-1]
        
    df = df.replace({np.NaN: None})

    check_difference_and_update_checkouts_full(df, checkouts_full, engine)
    check_difference_and_update_checkout_items(dfp, checkout_items, engine)

    return True

def writeCsvLog(CSV_FILE, level, description, message):
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp","level", "description", "message"])
    # Append to CSV log
    time_now = datetime.now(pytz.timezone('Chile/Continental')).isoformat()
    with open(CSV_FILE, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([time_now,level, description, message])

def encrypt(data, key):
    f = Fernet(key.encode())
    encoded = data.encode()
    return f.encrypt(encoded)

def decrypt(encrypted, key):
    f = Fernet(key.encode())
    decrypted = f.decrypt(encrypted)
    return decrypted.decode()

def get_data_brands(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/brands/p/1"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except Exception as e:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    brands = pd.DataFrame(response["entries"])
    brands["type"] = "brand"
    brands = brands[["_id", "name", "type"]]
    return brands

def get_data_warranties(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/warranties"    
    headers = {
            'Authorization': f'Bearer {token}'
        }

    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    warranties = pd.DataFrame(response["entries"])
    warranties["type"] = "warranty"
    warranties = warranties[["_id", "name", "type"]]
    return warranties

def get_data_tags(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/tags/p/1"    
    headers = {
            'Authorization': f'Bearer {token}'
        }
    
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    if response['pagination']['total_pages'] > 1:
        data = []
        for i in range(response['pagination']['total_pages']):
            url = f"https://app.multivende.com/api/m/{merchant_id}/tags/p/{i+1}"
            response2 = requests.request("GET", url, headers=headers).json()
            data += response2['entries']
    else:
        data = response['entries']
        
    tags = pd.DataFrame(data)
        
    tags = pd.DataFrame(response["entries"])
    tags["type"] = "tag"
    tags = tags[["_id", "name", "type"]]
    return tags

def get_data_colors(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/colors/p/1"    
    headers = {
            'Authorization': f'Bearer {token}'
        }

    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    colors = pd.DataFrame(response["entries"])
    colors["type"] = "color"
    colors = colors[["_id", "name", "type"]]
    return colors

def get_data_categories(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/product-categories/p/1"    
    headers = {
            'Authorization': f'Bearer {token}'
        }
    
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
    
    pages = response["pagination"]["total_pages"]
    data = []
    for p in range(pages):
        url = f"https://app.multivende.com/api/m/{merchant_id}/product-categories/p/{p+1}"
        headers = {
                'Authorization': f'Bearer {token}'
            }
        response = requests.request("GET", url, headers=headers)
        
        try:
            response = response.json()
        except:
            logger.error(f"Error: {response.text}")
            sys.exit()
        
        df = pd.DataFrame(response["entries"])
        data.append(df)
        
    cats = pd.concat(data, ignore_index=True)
    cats["type"] = "category"
    cats = cats[["_id", "name", "type"]]
    return cats

def get_data_size(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/sizes/p/1"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
    
    size = pd.DataFrame(response["entries"])
    size["type"] = "size"
    size = size[["_id", "name", "type"]]
    return size

def get_customs_attributes(token, merchant_id):
    url1 = f"https://app.multivende.com/api/m/{merchant_id}/custom-attribute-sets/products"
    #url1 = f"https://app.multivende.com/api/m/{merchant_id}/all-product-attributes"
    url2 = f"https://app.multivende.com/api/m/{merchant_id}/custom-attribute-sets/product_versions"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response1 = requests.request("GET", url1, headers=headers)
    response2 = requests.request("GET", url2, headers=headers)
    try:
        response1 = response1.json()
    except:
        logger.error(f"Error: {response1.text}")
        sys.exit()
    try:
        response2 = response2.json()
    except:
        logger.error(f"Error: {response2.text}")
        sys.exit()

    logger.info('Procesando atributos de productos.')
    info_p = []
    print("Respuesta 1: ", response1)
    for item in response1["entries"]:
        custom_att = {}
        custom_att["id_set"] = item["_id"]
        custom_att["name_set"] = item["name"]
        if len(item["CustomAttributes"]) == 0:
            custom_att["id"] = None
            custom_att["name"] = None
            custom_att["option_name"] = None
            custom_att["option_id"] = None
            info_p.append(custom_att)
            continue
        for ca in item["CustomAttributes"]:
            custom_att_p = custom_att.copy()
            custom_att_p["id"] = ca["_id"]
            custom_att_p["name"] = ca["name"]
            if ca["CustomAttributeType"]['_id'] != '763c2831-b9af-462f-8974-d401f358949c':
                custom_att_p["option_name"] = None
                custom_att_p["option_id"] = None
                info_p.append(custom_att_p)
                continue
            for op in ca["CustomAttributeOptions"]:
                custom_att_op = custom_att_p.copy()
                custom_att_op["option_name"] = op["text"]
                custom_att_op["option_id"] = op["_id"]
                info_p.append(custom_att_op)
                
                
    dfp = pd.DataFrame(info_p)

    logger.info('Procesando atributos para versiones de productos.')
    info_pv = []
    for item in response2["entries"]:
        custom_att = {}
        custom_att["id_set"] = item["_id"]
        custom_att["name_set"] = item["name"]
        if len(item["CustomAttributes"]) == 0:
            custom_att["id"] = None
            custom_att["name"] = None
            custom_att["option_name"] = None
            custom_att["option_id"] = None
            info_pv.append(custom_att)
            continue
        for ca in item["CustomAttributes"]:
            custom_att_p = custom_att.copy()
            custom_att_p["id"] = ca["_id"]
            custom_att_p["name"] = ca["name"]
            if ca["CustomAttributeType"]['_id'] != '763c2831-b9af-462f-8974-d401f358949c':
                custom_att_p["option_name"] = None
                custom_att_p["option_id"] = None
                info_pv.append(custom_att_p)
                continue
            for op in ca["CustomAttributeOptions"]:
                custom_att_op = custom_att_p.copy()
                custom_att_op["option_name"] = op["text"]
                custom_att_op["option_id"] = op["_id"]
                info_pv.append(custom_att_op)
                
    dfv = pd.DataFrame(info_pv)
    
    data = pd.concat([dfv, dfp], ignore_index=True)
    return data

def upload_data_products(df, Product, Attributes, engine):    
    # This two attribute columns are duplicated, remove one
    try:
        df.drop(df.columns[df.columns.str.contains("Material del trípode")][1], axis=1, inplace=True)
    except:
        logger.warning("The column: Material del trípode. NO se encuentra en el dataframe")
        
    try:
        df.drop("Número de focos-Ripley Productos", axis=1, inplace=True)
    except:
        logger.warning("The column: Número de focos-Ripley Productos. NO se encuentra en el dataframe")

    # Split dataframe by marketplaces and upload data to each one
    logger.info('Subiendo a la DB.')
    df = df.replace({np.nan: None})
    try:
        with Session(engine) as session:
            for id, row in df.iterrows():
                # Get product data if exists
                result = session.scalar(select(Product).where(Product.id_padre == row['IDENTIFICADOR_PADRE'] and 
                                                              Product.id_hijo == row['IDENTIFICADOR_HIJO']))
                # For each attribute of product check if key/value is in DB
                atts = row[23:-13]
                attributes = []
                for i in atts[atts.notna()].index:
                    # If is number create the correct object
                    #print("Atts [i] ",atts[i], "\n")
                    att_value = str(atts[i])
                    if  att_value.replace('.', '').isdigit():
                        attribute = Attributes(name = i, number_value = float(atts[i]))
                        # Check if it's in DB
                        exists_criteria = (select(Attributes.id)
                                           .where(Attributes.name == attribute.name and 
                                                  Attributes.number_value == attribute.number_value)
                                           .exists())
                        exists_att = session.scalar(select(exists_criteria)) 
                        # Add the item to temporal list of attributes of the product
                        attributes.append(attribute)
                    else:
                        attribute = Attributes(name = i, text_value = atts[i])
                        # Check if it's in DB
                        exists_criteria = (select(Attributes.id)
                                           .where(Attributes.name == attribute.name and 
                                                  Attributes.text_value == attribute.text_value)
                                           .exists())
                        exists_att = session.scalar(select(exists_criteria))
                        # Add to the list of the product
                        attributes.append(attribute)
                    if exists_att:
                        # Add the key/value attribute if it's not in DB
                        session.add(attribute)
                if result == None:
                    # If new product, add it to DB
                    new_product = Product(id_padre = row['IDENTIFICADOR_PADRE'], 
                                          id_hijo= row['IDENTIFICADOR_HIJO'],
                                          season = row['Season'],
                                          model = row['model'],
                                          description = row['description'],
                                          htmlDescription = row['htmlDescription'],
                                          shortDescription = row['shortDescription'],
                                          htmlShortDescription = row['htmlShortDescription'],
                                          warranty = row['Warranty'],
                                          brand = row['Brand'],
                                          name = row['name'],
                                          productCategory = row['ProductCategory'],
                                          skuName = row['sku_name'],
                                          color = row['color'],
                                          size = row['size'],
                                          sku = row['sku'],
                                          internalSku = row['internalSku'],
                                          width = row['width'],
                                          length = row['length'],
                                          stock = 0,
                                          height = row['height'],
                                          weight = row['weight'],
                                          tags = row['tags'],
                                          picture = row['picture url'])
                    for attribute in attributes:
                        # Associate the attributes objects to the new product
                        new_product.attributes.append(attribute)
                    session.add(new_product)
                else:
                    # If the product exists, reset attributes links
                    for i in range(len(result.attributes)):
                        result.attributes.pop()
                    # And a new ones (update)
                    for attribute in attributes:
                        result.attributes.append(attribute)
                    # Update product info
                    stmt = (update(Product)
                                 .where(Product.id_padre == row['IDENTIFICADOR_PADRE'] and 
                                        Product.id_hijo == row['IDENTIFICADOR_HIJO'])
                                 .values(season = row['Season'],
                                         model = row['model'],
                                         description = row['description'],
                                         htmlDescription = row['htmlDescription'],
                                         shortDescription = row['shortDescription'],
                                         htmlShortDescription = row['htmlShortDescription'],
                                         warranty = row['Warranty'],
                                         brand = row['Brand'],
                                         name = row['name'],
                                         productCategory = row['ProductCategory'],
                                         skuName = row['sku_name'],
                                         color = row['color'],
                                         size = row['size'],
                                         sku = row['sku'],
                                         internalSku = row['internalSku'],
                                         width = row['width'],
                                         length = row['length'],
                                         height = row['height'],
                                         stock = 0,
                                         weight = row['weight'],
                                         tags = row['tags'],
                                         picture = row['picture url']))
                    session.execute(stmt)

            session.commit()
        logger.info(f"Tabla 'Productos' populada con exito.")
    except Exception as e:
        logger.error(f"La tabla 'Productos' tuvo un error {e}")
        sys.exit(0)

def check_diferences_and_update_deliverys(CSV_FILE, data, deliverys, engine):
    """Funcion para actualizacion de despachos.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con las entregas a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      entregas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """
    updated_counter = 0
    created_counter = 0

    # Check if the delivery is in the DB
    with Session(engine) as session:
        for i, row in data.iterrows():
            result = session.scalar(select(deliverys).where(deliverys.id_venta == row["id venta"] and 
                                                              deliverys.n_venta == row["n venta"]))
            # Add the new delivery to the DB
            try:
                if result == None:
                    delivery = deliverys(n_seguimiento = row["N seguimiento"], codigo = row["codigo"],
                                codigo_venta = row["codigo venta"], courier = row["courier"], clase_de_envio = row["clase de envio"],
                                delivery_status = row["delivery status"], direccion = row["direccion"],
                                impresion_etiqueta = row["estado impresion etiqueta"], fecha_despacho = row["fecha despacho"],
                                fecha_promesa = row["fecha promesa"], id_venta = row["id venta"], 
                                status_etiqueta = row["status etiqueta"], n_venta = row["n venta"])
                    session.add(delivery)
                    created_counter = created_counter + 1 
                # update old values                         
                else:
                    stmt = (
                        update(deliverys)
                        .where(deliverys.id_venta == row["id venta"] and 
                            deliverys.n_venta == row["n venta"])
                        .values(n_seguimiento = row["N seguimiento"], codigo = row["codigo"],
                                codigo_venta = row["codigo venta"], courier = row["courier"], clase_de_envio = row["clase de envio"],
                                delivery_status = row["delivery status"], direccion = row["direccion"],
                                impresion_etiqueta = row["estado impresion etiqueta"], fecha_despacho = row["fecha despacho"],
                                fecha_promesa = row["fecha promesa"], id_venta = row["id venta"], 
                                status_etiqueta = row["status etiqueta"], n_venta = row["n venta"])
                    )
                    session.execute(stmt)
                    updated_counter = updated_counter + 1
            except Exception as e:
                writeCsvLog(CSV_FILE, "ERROR", "DB loading error", f"The data load has failed {e}")
                sys.exit(0)            
        session.commit()
        writeCsvLog(CSV_FILE, "INFO", "Upload info", f"Rows created {created_counter} Rows updated {updated_counter}")
        logger.error(f"Rows created {created_counter} Rows updated {updated_counter}")

def check_difference_and_update_checkouts(CSV_FILE, data, checkouts, engine):
    """Funcion para actualizacion de ventas/checkouts.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con los checkouts a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      ventas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """

    updated_counter = 0
    created_counter = 0

    # Check if the checkout is in the DB
    with Session(engine) as session:
        for i, row in data.iterrows():
            if row["nombre"] == None:
                continue
            result = session.scalar(select(checkouts).where(checkouts.id_venta == row["id"] and 
                                                              checkouts.id_hijo_producto == row["id hijo producto"]))
            try:
            # Add the new checkout to the DB
                if result == None:
                    venta = checkouts(cantidad = row["cantidad"], codigo_producto = row["codigo producto"],
                                costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                                estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                                fecha = row["fecha"], id_venta = row["id"], id_hijo_producto = row["id hijo producto"],
                                id_padre_producto = row["id padre producto"], mail = row["mail"], 
                                market = row["market"], n_venta = row["n venta"], 
                                nombre_cliente = row["nombre"], nombre_producto = row["nombre producto"],
                                phone = row["phone"], precio = row["precio"],
                                url_boleta = row["url boleta"])
                    session.add(venta)
                    created_counter = created_counter + 1 
                # Update the old values
                else:
                    stmt = (
                        update(checkouts)
                        .where(checkouts.id_venta == row["id"] and 
                            checkouts.id_hijo_producto == row["id hijo producto"])
                        .values(cantidad = row["cantidad"], codigo_producto = row["codigo producto"],
                                costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                                estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                                fecha = row["fecha"], id_venta = row["id"], id_hijo_producto = row["id hijo producto"],
                                id_padre_producto = row["id padre producto"], mail = row["mail"], 
                                market = row["market"], n_venta = row["n venta"], 
                                nombre_cliente = row["nombre"], nombre_producto = row["nombre producto"],
                                phone = row["phone"], precio = row["precio"],
                                url_boleta = row["url boleta"])
                    )
                    session.execute(stmt)
                    updated_counter = updated_counter + 1
            except Exception as e:
                writeCsvLog(CSV_FILE, "ERROR", "DB loading error", f"The data load has failed {e}")
                sys.exit(0)
        session.commit()
        writeCsvLog(CSV_FILE, "INFO", "Upload info", f"Rows created {created_counter} Rows updated {updated_counter}")
        logger.error(f"Rows created {created_counter} Rows updated {updated_counter}")

def check_difference_and_update_checkouts_full(data, checkouts_full, engine):
    """Funcion para actualizacion de ventas/checkouts.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con los checkouts a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      ventas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """

    updated_counter = 0
    created_counter = 0

    # Check if the checkout is in the DB
    print("Cargando info...")
    with Session(engine) as session:
        print("Sesion iniciada..")
        for i, row in data.iterrows():
            if row["nombre"] == None:
                continue
            result = session.scalar(select(checkouts_full).where(checkouts_full.id_venta == row["id"]))
            print("Base de datos consultada..")
            try:
            # Add the new checkout to the DB
                if result == None:
                    venta = checkouts_full(costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                                estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                                fecha = row["fecha"],mail = row["mail"], 
                                market = row["market"], n_venta = row["n venta"], 
                                nombre_cliente = row["nombre"],
                                phone = row["phone"],
                                url_boleta = row["url boleta"],n_seguimiento = row["N seguimiento"], codigo = row["codigo"],
                                codigo_venta = row["codigo venta"], courier = row["courier"], clase_de_envio = row["clase de envio"],
                                delivery_status = row["delivery status"], direccion = row["direccion"],
                                impresion_etiqueta = row["estado impresion etiqueta"], fecha_despacho = row["fecha despacho"],
                                fecha_promesa = row["fecha promesa"], id_venta = row["id venta"], 
                                status_etiqueta = row["status etiqueta"])
                    session.add(venta)
                    created_counter = created_counter + 1
                    print("Creando item..") 
                # Update the old values
                else:
                    stmt = (
                        update(checkouts_full)
                        .where(checkouts_full.id_venta == row["id"])
                        .values(costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                                estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                                fecha = row["fecha"],mail = row["mail"], 
                                market = row["market"], n_venta = row["n venta"], 
                                nombre_cliente = row["nombre"],
                                phone = row["phone"],
                                url_boleta = row["url boleta"],n_seguimiento = row["N seguimiento"], codigo = row["codigo"],
                                codigo_venta = row["codigo venta"], courier = row["courier"], clase_de_envio = row["clase de envio"],
                                delivery_status = row["delivery status"], direccion = row["direccion"],
                                impresion_etiqueta = row["estado impresion etiqueta"], fecha_despacho = row["fecha despacho"],
                                fecha_promesa = row["fecha promesa"], id_venta = row["id venta"], 
                                status_etiqueta = row["status etiqueta"])
                    )
                    session.execute(stmt)
                    updated_counter = updated_counter + 1
                    print("Actualizando item..") 
            except Exception as e:
                print(e)
                sys.exit(0)
        session.commit()
        print("Info cargada exitosamente...")
        #writeCsvLog(CSV_FILE, "INFO", "Upload info", f"Rows created {created_counter} Rows updated {updated_counter}")
        logger.error(f"Rows created {created_counter} Rows updated {updated_counter}")

def check_difference_and_update_checkout_items(data, checkout_items, engine):
    """Funcion para actualizacion de los items de los checkouts.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con los checkouts a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      ventas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """

    updated_counter = 0
    created_counter = 0

    # Check if the checkout is in the DB
    print("Cargando info...")
    with Session(engine) as session:
        print("Sesion iniciada para productos..")
        for i, row in data.iterrows():
            if row["nombre producto"] == None:
                continue
            result = session.scalar(select(checkout_items).where(checkout_items.id_venta == row["id venta"]).where(checkout_items.id_hijo_producto == row["id hijo producto"]))                
            print("Base de datos consultada..")
            try:
            # Add the new checkout to the DB
                if result == None:
                    item = checkout_items(
                                codigo_producto = row["codigo producto"],
                                nombre_producto = row["nombre producto"],
                                id_padre_producto = row["id padre producto"],
                                id_hijo_producto = row["id hijo producto"],
                                cantidad = row["cantidad"],
                                precio = row["precio"],
                                id_venta = row["id venta"]
                                )
                    session.add(item)
                    created_counter = created_counter + 1
                    print("Creando item..") 
                # Update the old values
                else:
                    stmt = (
                        update(checkout_items)
                        .where(checkout_items.id_venta == row["id venta"] and checkout_items.id_hijo_producto == row["id hijo producto"])
                        .values(
                                codigo_producto = row["codigo producto"],
                                nombre_producto = row["nombre producto"],
                                id_padre_producto = row["id padre producto"],
                                id_hijo_producto = row["id hijo producto"],
                                cantidad = row["cantidad"], 
                                precio = row["precio"],
                                id_venta = row["id venta"]
                                )
                    )
                    session.execute(stmt)
                    updated_counter = updated_counter + 1
                    print("Actualizando item..") 
            except Exception as e:
                writeCsvLog(CSV_FILE, "ERROR", "DB loading error", f"The data load has failed {e}")
                sys.exit(0)
        session.commit()
        print("Info cargada exitosamente...")
        #writeCsvLog(CSV_FILE, "INFO", "Upload info", f"Rows created {created_counter} Rows updated {updated_counter}")
        logger.error(f"Rows created {created_counter} Rows updated {updated_counter}")

def upsert_checkout_full(data, checkouts_full):
    """Funcion para actualizar un item individual de checkouts.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Diccionario de datos con la informacion de checkout a actualizar.

      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 

    Output :
    ---------
      * None.
    """

    engine = create_engine(SQLALCHEMY_DATABASE_URI,
                        pool_recycle=3600,   # recycle connections every hour
                        pool_pre_ping=True,
                        connect_args={
                            "ssl_ca": ssl
                            }
                        )

    # Check if the checkout is in the DB
    print("Cargando info...")
    with Session(engine) as session:
        result = session.scalar(select(checkouts_full).where(checkouts_full.id_venta == data["id venta"]))
        print("Sesion iniciada para productos..")
        try:
        # Add the new checkout to the DB
            print("Intentando...")
            if result == None:
                venta = checkouts_full(costo_envio = data["costo de envio"], estado_boleta = data["estado boleta"],
                            estado_entrega = data["estado entrega"], estado_venta = data["estado venta"],
                            fecha = data["fecha"],mail = data["mail"], 
                            market = data["market"], n_venta = data["n venta"], 
                            nombre_cliente = data["nombre"],
                            phone = data["phone"],
                            url_boleta = data["url boleta"], codigo = data["codigo"],
                            codigo_venta = data["codigo venta"], courier = data["courier"], clase_de_envio = data["clase de envio"],
                            delivery_status = data["delivery status"], direccion = data["direccion"],
                            impresion_etiqueta = data["estado impresion etiqueta"], fecha_despacho = data["fecha despacho"],
                            fecha_promesa = data["fecha promesa"], id_venta = data["id venta"], 
                            status_etiqueta = data["status etiqueta"])
                session.add(venta)
                print("Creando item..") 
            # Update the old values
            else:
                stmt = (
                    update(checkouts_full)
                    .where(checkouts_full.id_venta == data["id"])
                    .values(costo_envio = data["costo de envio"], estado_boleta = data["estado boleta"],
                            estado_entrega = data["estado entrega"], estado_venta = data["estado venta"],
                            fecha = data["fecha"],mail = data["mail"], 
                            market = data["market"], n_venta = data["n venta"], 
                            nombre_cliente = data["nombre"],
                            phone = data["phone"],
                            url_boleta = data["url boleta"], codigo = data["codigo"],
                            codigo_venta = data["codigo venta"], courier = data["courier"], clase_de_envio = data["clase de envio"],
                            delivery_status = data["delivery status"], direccion = data["direccion"],
                            impresion_etiqueta = data["estado impresion etiqueta"], fecha_despacho = data["fecha despacho"],
                            fecha_promesa = data["fecha promesa"], id_venta = data["id venta"], 
                            status_etiqueta = data["status etiqueta"])
                )
                session.execute(stmt)
                print("Actualizando item..") 
        except Exception as e:
            print("Hubo un error ", e)
            sys.exit(0)
    session.commit()
    print("Info cargada exitosamente...")
    #writeCsvLog(CSV_FILE, "INFO", "Upload info", f"Rows created {created_counter} Rows updated {updated_counter}")