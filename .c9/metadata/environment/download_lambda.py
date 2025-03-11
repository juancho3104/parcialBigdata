{"filter":false,"title":"download_lambda.py","tooltip":"/download_lambda.py","undoManager":{"mark":4,"position":4,"stack":[[{"start":{"row":0,"column":0},"end":{"row":146,"column":0},"action":"insert","lines":["import os                      # Módulo para interactuar con el sistema (por ejemplo, variables de entorno)","import json                    # Módulo para manejar datos JSON (aunque en este caso no lo usamos)","import re                      # Módulo para expresiones regulares (para extraer datos si fuese necesario)","import requests                # Módulo para hacer peticiones HTTP","import boto3                   # Módulo de AWS para interactuar con servicios como S3","import csv                     # Módulo para escribir archivos CSV","from io import StringIO        # StringIO para crear buffers en memoria para el CSV","from datetime import datetime  # Para obtener la fecha actual","from bs4 import BeautifulSoup  # Para parsear el HTML","","# Creamos un cliente S3 para interactuar con el bucket","s3_client = boto3.client(\"s3\")","","# Definimos un header que simula un navegador real (User-Agent)","HEADERS = {","    \"User-Agent\": (","        \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \"","        \"(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36\"","    )","}","","def download_handler(event, context):","    \"\"\"","    Descarga las primeras 10 páginas de resultados desde Mitula y guarda el HTML","    completo en un bucket S3.","    \"\"\"","    base_url = (\"https://casas.mitula.com.co/find?\"","                \"operationType=sell&propertyType=mitula_studio_apartment&\"","                \"geoId=mitula-CO-poblacion-0000014156&\"","                \"text=Bogotá%2C++%28Cundinamarca%29\")","    html_content = \"\"","","    # Recorremos las páginas 1 a 10","    for page in range(1, 11):","        url = f\"{base_url}&page={page}\"","        response = requests.get(url, headers=HEADERS)  # Petición GET con header simulado","        if response.status_code == 200:","            html_content += response.text + \"\\n\"  # Acumulamos el contenido HTML","        else:","            print(f\"Error al descargar la página {page}: {response.status_code}\")","","    # Obtenemos la fecha actual para nombrar el archivo","    today = datetime.utcnow().strftime(\"%Y-%m-%d\")","    filename = f\"{today}.html\"","    # Obtenemos el bucket desde variable de entorno o usamos \"parcial\" por defecto","    bucket = os.environ.get(\"S3_BUCKET\", \"parcial\")","","    # Subimos el HTML acumulado al bucket S3","    s3_client.put_object(","        Bucket=bucket,","        Key=filename,","        Body=html_content,","        ContentType=\"text/html\"","    )","    print(f\"Archivo {filename} subido al bucket {bucket}\")","    return {\"status\": \"downloaded\", \"filename\": filename}","","def process_handler(event, context):","    \"\"\"","    Procesa el HTML descargado de S3 realizando scraping de cada listado.","    Extrae los siguientes datos:","      - Valor: precio (extraído de <span class=\"price__actual\">)","      - Barrio: la ubicación (extraído de <div class=\"listing-card__location__geo\">)","      - NumHabitaciones: número de habitaciones (del atributo content de <p data-test=\"bedrooms\">)","      - NumBanos: número de baños (del atributo content de <p data-test=\"bathrooms\">)","      - mts2: área en metros cuadrados (del atributo content de <p data-test=\"floor-area\">)","    La fila CSV tendrá las columnas:","      FechaDescarga, Barrio, Valor, NumHabitaciones, NumBanos, mts2","    \"\"\"","    # 1. Obtener el registro del evento S3","    record = event[\"Records\"][0]","    source_bucket = record[\"s3\"][\"bucket\"][\"name\"]  # Bucket fuente","    object_key = record[\"s3\"][\"object\"][\"key\"]        # Nombre del archivo HTML en S3","","    # 2. Descargar el archivo HTML desde S3","    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)","    html_content = response[\"Body\"].read().decode(\"utf-8\")","","    # 3. Parsear el HTML usando BeautifulSoup","    soup = BeautifulSoup(html_content, \"html.parser\")","    # Buscar todos los contenedores de listado usando la clase \"listing-card__content\"","    listings = soup.find_all(\"div\", class_=\"listing-card__content\")","    if not listings:","        print(\"No se encontraron listados con la clase 'listing-card__content'\")","        return {\"status\": \"error\", \"message\": \"No listings found\"}","","    rows = []  # Lista para almacenar las filas del CSV","    # Se utiliza el nombre del archivo (sin extensión) como fecha de descarga","    fecha_descarga = object_key.replace(\".html\", \"\")","","    # 4. Iterar sobre cada listado encontrado","    for listing in listings:","        # Extraer el precio: buscamos el <span> con clase \"price__actual\"","        price_elem = listing.find(\"span\", class_=\"price__actual\")","        valor = price_elem.get_text(strip=True) if price_elem else \"N/A\"","","        # Extraer la ubicación (Barrio): buscamos el <div> con clase \"listing-card__location__geo\"","        loc_elem = listing.find(\"div\", class_=\"listing-card__location__geo\")","        barrio = loc_elem.get_text(strip=True) if loc_elem else \"N/A\"","","        # Extraer número de habitaciones: <p data-test=\"bedrooms\"> (se obtiene del atributo \"content\")","        bed_elem = listing.find(\"p\", {\"data-test\": \"bedrooms\"})","        num_habitaciones = bed_elem.get(\"content\", \"N/A\") if bed_elem else \"N/A\"","","        # Extraer número de baños: <p data-test=\"bathrooms\"> (se obtiene del atributo \"content\")","        bath_elem = listing.find(\"p\", {\"data-test\": \"bathrooms\"})","        num_banos = bath_elem.get(\"content\", \"N/A\") if bath_elem else \"N/A\"","","        # Extraer área en metros cuadrados: <p data-test=\"floor-area\"> (atributo \"content\")","        area_elem = listing.find(\"p\", {\"data-test\": \"floor-area\"})","        mts2 = area_elem.get(\"content\", \"N/A\") if area_elem else \"N/A\"","","        # Agregar una fila con la información extraída","        rows.append([fecha_descarga, barrio, valor, num_habitaciones, num_banos, mts2])","","    # 5. Crear el CSV en memoria usando StringIO","    csv_buffer = StringIO()","    writer = csv.writer(csv_buffer)","    # Escribir la fila de cabecera","    writer.writerow([\"FechaDescarga\", \"Barrio\", \"Valor\", \"NumHabitaciones\", \"NumBanos\", \"mts2\"])","    # Escribir todas las filas de datos","    writer.writerows(rows)","","    # 6. Subir el CSV generado a S3","    dest_bucket = os.environ.get(\"DEST_BUCKET\", \"parcial\")","    csv_filename = object_key.replace(\".html\", \".csv\")","    s3_client.put_object(","        Bucket=dest_bucket,","        Key=csv_filename,","        Body=csv_buffer.getvalue(),","        ContentType=\"text/csv\"","    )","    print(f\"Archivo CSV {csv_filename} subido al bucket {dest_bucket}\")","    return {\"status\": \"processed\", \"csv_filename\": csv_filename}","","def lambda_handler(event, context):","    \"\"\"","    Función handler principal que decide qué función ejecutar según el evento.","    Si el evento contiene registros S3 (clave \"Records\" con un subobjeto \"s3\"),","    se ejecuta process_handler (para procesar el HTML y generar el CSV).","    De lo contrario, se ejecuta download_handler (para descargar el HTML).","    \"\"\"","    if \"Records\" in event and event[\"Records\"] and \"s3\" in event[\"Records\"][0]:","        return process_handler(event, context)","    else:","        return download_handler(event, context)",""],"id":1}],[{"start":{"row":45,"column":49},"end":{"row":45,"column":50},"action":"insert","lines":["s"],"id":2}],[{"start":{"row":124,"column":56},"end":{"row":124,"column":57},"action":"insert","lines":["s"],"id":3},{"start":{"row":124,"column":57},"end":{"row":124,"column":58},"action":"insert","lines":["s"]}],[{"start":{"row":124,"column":57},"end":{"row":124,"column":58},"action":"remove","lines":["s"],"id":4}],[{"start":{"row":146,"column":0},"end":{"row":147,"column":0},"action":"insert","lines":["",""],"id":5}]]},"ace":{"folds":[],"scrolltop":0,"scrollleft":0,"selection":{"start":{"row":26,"column":17},"end":{"row":29,"column":51},"isBackwards":true},"options":{"guessTabSize":true,"useWrapMode":false,"wrapToView":true},"firstLineState":{"row":46,"state":"start","mode":"ace/mode/python"}},"timestamp":1741659372485,"hash":"9a4917e1375cb8932f0fe2751399941ea371e8f1"}