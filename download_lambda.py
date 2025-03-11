import os
import json
import re
import requests
import boto3
import csv
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup

# Creamos un cliente S3 para interactuar con el bucket
s3_client = boto3.client("s3")

# Definimos un header que simula un navegador real (User-Agent)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/110.0.0.0 Safari/537.36"
    )
}


def download_handler(event, context):
    """
    Descarga las primeras 10 páginas de resultados desde Mitula y guarda
    el HTML completo en un bucket S3.
    """
    base_url = (
        "https://casas.mitula.com.co/find?"
        "operationType=sell&propertyType=mitula_studio_apartment&"
        "geoId=mitula-CO-poblacion-0000014156&"
        "text=Bogot%C3%A1%2C++%28Cundinamarca%29"
    )
    html_content = ""

    # Recorremos las páginas 1 a 10
    for page in range(1, 11):
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            html_content += response.text + "\n"
        else:
            print(f"Error al descargar la página {page}: "
                  f"{response.status_code}")

    # Obtenemos la fecha actual para nombrar el archivo
    today = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"{today}.html"
    bucket = os.environ.get("S3_BUCKET", "parcials")

    # Subimos el HTML acumulado al bucket S3
    s3_client.put_object(
        Bucket=bucket,
        Key=filename,
        Body=html_content,
        ContentType="text/html"
    )
    print(f"Archivo {filename} subido al bucket {bucket}")
    return {"status": "downloaded", "filename": filename}


def process_handler(event, context):
    """
    Procesa el HTML descargado de S3 realizando scraping de cada listado.
    Extrae los siguientes datos:
      - Valor: precio (extraído de <span class="price__actual">)
      - Barrio: ubicación (extraído de
        <div class="listing-card__location__geo">)
      - NumHabitaciones: número de habitaciones (del atributo "content" en
        <p data-test="bedrooms">)
      - NumBanos: número de baños (del atributo "content" en
        <p data-test="bathrooms">)
      - mts2: área en metros cuadrados (del atributo "content" en
        <p data-test="floor-area">)
    La fila CSV tendrá las columnas:
      FechaDescarga, Barrio, Valor, NumHabitaciones, NumBanos, mts2
    """
    # 1. Obtener el registro del evento S3
    record = event["Records"][0]
    source_bucket = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    # 2. Descargar el archivo HTML desde S3
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    html_content = response["Body"].read().decode("utf-8")

    # 3. Parsear el HTML usando BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")
    listings = soup.find_all("div", class_="listing-card__content")
    if not listings:
        print("No se encontraron listados con la clase "
              "'listing-card__content'")
        return {"status": "error", "message": "No listings found"}

    rows = []
    fecha_descarga = object_key.replace(".html", "")

    # 4. Iterar sobre cada listado
    for listing in listings:
        # Precio
        price_elem = listing.find("span", class_="price__actual")
        valor = price_elem.get_text(strip=True) if price_elem else "N/A"

        # Barrio / Ubicación
        loc_elem = listing.find("div", class_="listing-card__location__geo")
        barrio = loc_elem.get_text(strip=True) if loc_elem else "N/A"

        # Número de habitaciones
        bed_elem = listing.find("p", {"data-test": "bedrooms"})
        num_habitaciones = (bed_elem.get("content", "N/A")
                            if bed_elem else "N/A")

        # Número de baños
        bath_elem = listing.find("p", {"data-test": "bathrooms"})
        num_banos = bath_elem.get("content", "N/A") if bath_elem else "N/A"

        # Área en metros cuadrados
        area_elem = listing.find("p", {"data-test": "floor-area"})
        mts2 = area_elem.get("content", "N/A") if area_elem else "N/A"

        rows.append([
            fecha_descarga, barrio, valor, num_habitaciones, num_banos, mts2
        ])

    # 5. Crear el CSV en memoria usando StringIO
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow([
        "FechaDescarga", "Barrio", "Valor",
        "NumHabitaciones", "NumBanos", "mts2"
    ])
    writer.writerows(rows)

    # 6. Subir el CSV generado a S3
    dest_bucket = os.environ.get("DEST_BUCKET", "parcials")
    csv_filename = object_key.replace(".html", ".csv")
    s3_client.put_object(
        Bucket=dest_bucket,
        Key=csv_filename,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    print(f"Archivo CSV {csv_filename} subido al bucket {dest_bucket}")
    return {"status": "processed", "csv_filename": csv_filename}


def lambda_handler(event, context):
    """
    Función handler principal que decide qué función ejecutar según el evento.
    Si el evento contiene registros S3 (con la clave "Records" y subobjeto "s3"),
    se ejecuta process_handler para procesar el HTML y generar el CSV; de lo
    contrario, se ejecuta download_handler para descargar el HTML.
    """
    if ("Records" in event and event["Records"] and
            "s3" in event["Records"][0]):
        return process_handler(event, context)
    return download_handler(event, context)
