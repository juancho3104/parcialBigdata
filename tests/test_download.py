import sys
import os

# Insertamos la carpeta raíz al inicio de sys.path
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

import json
import pytest
from datetime import datetime

# Ahora sí podemos importar la función desde download_lambda.py
from download_lambda import download_handler


class DummyResponse:
    """Clase dummy para simular la respuesta de requests.get."""
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


def dummy_get_success(url, headers=None):
    """Función dummy que simula una respuesta exitosa (200)."""
    return DummyResponse(200, f"<html>Content from {url}</html>")


def dummy_get_failure(url, headers=None):
    """
    Función dummy que simula una respuesta fallida (404) en la página 5.
    """
    if "page=5" in url:
        return DummyResponse(404, "Not Found")
    return DummyResponse(200, f"<html>Content from {url}</html>")


class DummyS3Client:
    """Clase dummy para simular el cliente S3."""
    def __init__(self):
        self.put_calls = []

    def put_object(self, Bucket, Key, Body, ContentType):
        """
        Almacena la información de la llamada para poder hacer aserciones.
        """
        self.put_calls.append({
            "Bucket": Bucket,
            "Key": Key,
            "Body": Body,
            "ContentType": ContentType,
        })
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@pytest.fixture
def dummy_s3_client(monkeypatch):
    """
    Fixture para reemplazar el objeto s3_client del módulo con
    nuestro dummy.
    """
    dummy = DummyS3Client()
    monkeypatch.setattr("download_lambda.s3_client", dummy)
    return dummy


def test_download_handler_success(monkeypatch, dummy_s3_client):
    """
    PRUEBA 1: Verificar que se genera el HTML correctamente cuando
    todas las páginas responden 200.
    """
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_success)
    monkeypatch.setenv("S3_BUCKET", "parcials")

    result = download_handler({}, None)
    assert result["status"] == "downloaded"
    assert len(dummy_s3_client.put_calls) == 1

    today = datetime.utcnow().strftime("%Y-%m-%d")
    expected_filename = f"{today}.html"
    assert result["filename"] == expected_filename

    body = dummy_s3_client.put_calls[0]["Body"]
    for page in range(1, 11):
        url = (
            "https://casas.mitula.com.co/find?"
            "operationType=sell&propertyType=mitula_studio_apartment&"
            "geoId=mitula-CO-poblacion-0000014156&"
            "text=Bogot%C3%A1%2C++%28Cundinamarca%29&page={}"
        ).format(page)
        assert f"Content from {url}" in body


def test_download_handler_partial_failure(monkeypatch, dummy_s3_client,
                                          capsys):
    """
    PRUEBA 2: Verificar el comportamiento cuando una de las páginas falla
    (por ejemplo, page=5).
    """
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_failure)
    monkeypatch.setenv("S3_BUCKET", "parcials")

    result = download_handler({}, None)
    captured = capsys.readouterr().out
    assert "Error al descargar la página 5" in captured
    assert result["status"] == "downloaded"


def test_download_handler_default_bucket(monkeypatch, dummy_s3_client):
    """
    PRUEBA 3: Verificar que si la variable de entorno S3_BUCKET no está
    definida se use un valor por defecto.
    """
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_success)

    result = download_handler({}, None)
    bucket_used = dummy_s3_client.put_calls[0]["Bucket"]
    assert bucket_used == "parcials"
