import sys
import os
# Insertamos la carpeta raíz al inicio de sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import pytest
from datetime import datetime

# Ahora sí podemos importar la función desde download_lambda.py
from download_lambda import download_handler

# Creamos una clase dummy para simular la respuesta de requests.get
class DummyResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

# Función dummy que simula una respuesta exitosa (200)
def dummy_get_success(url, headers=None):
    return DummyResponse(200, f"<html>Content from {url}</html>")

# Función dummy que simula una respuesta fallida (404) en una página específica
def dummy_get_failure(url, headers=None):
    if "page=5" in url:
        return DummyResponse(404, "Not Found")
    return DummyResponse(200, f"<html>Content from {url}</html>")

# Creamos una clase dummy para simular el cliente S3
class DummyS3Client:
    def __init__(self):
        self.put_calls = []

    def put_object(self, Bucket, Key, Body, ContentType):
        # Almacena la información de la llamada para poder hacer aserciones
        self.put_calls.append({
            "Bucket": Bucket,
            "Key": Key,
            "Body": Body,
            "ContentType": ContentType
        })
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

# Fixture para reemplazar el objeto s3_client del módulo con nuestro dummy
@pytest.fixture
def dummy_s3_client(monkeypatch):
    dummy = DummyS3Client()
    monkeypatch.setattr("download_lambda.s3_client", dummy)
    return dummy

# PRUEBA 1: Verificar que se genera el HTML correctamente cuando todas las páginas responden 200
def test_download_handler_success(monkeypatch, dummy_s3_client):
    # Reemplazamos requests.get con nuestra función dummy_get_success
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_success)
    # Configuramos la variable de entorno para S3_BUCKET
    monkeypatch.setenv("S3_BUCKET", "parcials")
    
    # Ejecutamos la función de descarga sin evento (download_handler se ejecuta cuando no hay "Records")
    result = download_handler({}, None)
    
    # Comprobamos que el status sea "downloaded"
    assert result["status"] == "downloaded"
    
    # Verificamos que se llamó una vez a put_object (para subir el HTML)
    assert len(dummy_s3_client.put_calls) == 1
    
    # Verificamos que el nombre del archivo incluya la fecha de hoy y termine en ".html"
    today = datetime.utcnow().strftime("%Y-%m-%d")
    expected_filename = f"{today}.html"
    assert result["filename"] == expected_filename
    
    # Verificamos que en el Body se concatenó el contenido de 10 páginas
    body = dummy_s3_client.put_calls[0]["Body"]
    for page in range(1, 11):
        url = (f"https://casas.mitula.com.co/find?operationType=sell&propertyType=mitula_studio_apartment&"
               f"geoId=mitula-CO-poblacion-0000014156&text=Bogotá%2C++%28Cundinamarca%29&page={page}")
        assert f"Content from {url}" in body


# PRUEBA 2: Verificar comportamiento cuando una de las páginas falla (por ejemplo, page=5)
def test_download_handler_partial_failure(monkeypatch, dummy_s3_client, capsys):
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_failure)
    monkeypatch.setenv("S3_BUCKET", "parcials")
    
    result = download_handler({}, None)
    
    # Se espera que se imprima un error para la página 5
    captured = capsys.readouterr().out
    assert "Error al descargar la página 5" in captured
    
    # Aunque falle una página, la función sigue y retorna status "downloaded"
    assert result["status"] == "downloaded"

# PRUEBA 3: Verificar que si la variable de entorno S3_BUCKET no está definida se use un valor por defecto
def test_download_handler_default_bucket(monkeypatch, dummy_s3_client):
    # Eliminamos la variable de entorno S3_BUCKET (o la dejamos vacía)
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_success)
    
    result = download_handler({}, None)
    
    # Como en el código se usa: os.environ.get("S3_BUCKET", "parcials")
    # Si S3_BUCKET no existe, se debería usar "parcial" como bucket
    bucket_used = dummy_s3_client.put_calls[0]["Bucket"]
    assert bucket_used == "parcials"
