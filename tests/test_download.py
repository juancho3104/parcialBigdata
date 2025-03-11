import os
import sys
import pytest
from datetime import datetime

# Insertamos la carpeta raíz al inicio de sys.path para poder importar download_lambda
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from download_lambda import download_handler


class DummyResponse:
    """Clase dummy para simular la respuesta de requests.get."""
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


def dummy_get_success(url, headers=None):
    """Simula una respuesta exitosa (200) de requests.get."""
    return DummyResponse(200, f"<html>Content from {url}</html>")


def dummy_get_failure(url, headers=None):
    """
    Simula una respuesta fallida (404) para la página 5 y exitosa para las demás.
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
        Simula la subida de un objeto a S3 y almacena los datos de la llamada.
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
    Fixture que reemplaza el objeto s3_client en download_lambda por nuestro DummyS3Client.
    """
    dummy = DummyS3Client()
    monkeypatch.setattr("download_lambda.s3_client", dummy)
    return dummy


def test_download_handler_success(monkeypatch, dummy_s3_client):
    """
    PRUEBA 1: Verifica que se genere el HTML correctamente cuando todas las páginas responden 200.
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


def test_download_handler_partial_failure(monkeypatch, dummy_s3_client, capsys):
    """
    PRUEBA 2: Verifica que si una de las páginas falla (por ejemplo, page=5),
    se imprima el error y la función continúe.
    """
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_failure)
    monkeypatch.setenv("S3_BUCKET", "parcials")

    result = download_handler({}, None)
    captured = capsys.readouterr().out
    assert "Error al descargar la página 5" in captured
    assert result["status"] == "downloaded"


def test_download_handler_default_bucket(monkeypatch, dummy_s3_client):
    """
    PRUEBA 3: Verifica que, si la variable de entorno S3_BUCKET no está definida,
    se use el valor por defecto ("parcials").
    """
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setattr("download_lambda.requests.get", dummy_get_success)

    result = download_handler({}, None)
    # Aseguramos que el resultado también indique éxito
    assert result["status"] == "downloaded"
    bucket_used = dummy_s3_client.put_calls[0]["Bucket"]
    assert bucket_used == "parcials"
