from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime, today
from pathlib import Path
import csv
import requests
import os
import json
import gzip
from io import BytesIO


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["mendoza", "gbfs", "station_status"],
)
def consultar_api_mendoza():
    # Define tasks
    @task(
        outlets=[Asset("mendoza_station_status")]
    )
    def fetch_station_status() -> dict:

        url = "https://api.mendoza.smod.io/v1/gbfs/station_status.json"
        
        # Headers para manejar compresión
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        try:
            print(f"Consultando API: {url}")
            response = requests.get(url, headers=headers, timeout=30)
            
            print(f"Response code: {response.status_code} - Petición exitosa")
            
            # Verificar si la respuesta es exitosa
            response.raise_for_status()
            
            # Detectar si la respuesta está comprimida
            content_encoding = response.headers.get('content-encoding', '').lower()
            
            if content_encoding == 'gzip':
                print("Respuesta comprimida detectada (gzip)")
                # Descomprimir la respuesta
                compressed_data = BytesIO(response.content)
                with gzip.GzipFile(fileobj=compressed_data) as gz_file:
                    json_data = gz_file.read().decode('utf-8')
            else:
                print("Respuesta no comprimida")
                json_data = response.text
            
            # Parsear JSON
            payload = json.loads(json_data)
            
            # Mostrar información básica del JSON
            print("JSON Response obtenido exitosamente:")
            print(f"- Tamaño de respuesta: {len(json_data)} caracteres")
            print(f"- Claves principales: {list(payload.keys())}")
            
            # Si hay datos de estaciones, mostrar estadísticas
            if 'data' in payload and 'stations' in payload['data']:
                stations = payload['data']['stations']
                print(f"- Número de estaciones: {len(stations)}")
                
                # Mostrar algunas estaciones como ejemplo
                if stations:
                    print("- Ejemplo de estación:")
                    example_station = stations[0]
                    for key, value in example_station.items():
                        print(f"  {key}: {value}")
            
            return payload
            
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición HTTP: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Error al parsear JSON: {e}")
            raise
        except Exception as e:
            print(f"Error inesperado: {e}")
            raise

    @task
    def process_station_status(station_data: dict) -> str:
        # Crear directorio de salida
        repo_root = Path(__file__).resolve().parent.parent
        output_dir = repo_root / "include" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generar nombre de archivo con fecha
        current_date = today().format('YYYY-MM-DD')
        output_path = output_dir / f"station_status_{current_date}.json"
        
        try:
            # Guardar datos en archivo JSON
            with output_path.open(mode="w", encoding="utf-8") as f:
                json.dump(station_data, f, indent=2, ensure_ascii=False)
            
            file_size = output_path.stat().st_size
            print(f"Archivo JSON guardado en: {output_path}")
            print(f"Tamaño del archivo: {file_size:,} bytes")
            
            return str(output_path)
            
        except Exception as e:
            print(f"Error al guardar archivo: {e}")
            raise

    # Ejecutar las tareas en secuencia
    station_data = fetch_station_status()
    process_station_status(station_data)


# Instantiate the DAG
consultar_api_mendoza()     