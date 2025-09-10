"""
## Mendoza Stations ETL example DAG

Este DAG consulta la API GBFS de estaciones de bicicletas de Mendoza y guarda
la información en un archivo CSV en la carpeta `include/output` del proyecto.

Hay dos tareas: una para obtener los datos desde la API y otra para guardarlos
en CSV. Ambas están escritas en Python usando la TaskFlow API de Airflow.
"""

from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime, today
from pathlib import Path
import csv
import requests
import os


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["mendoza", "gbfs"],
)
def mendoza_stations():
    # Define tasks
    @task(
        outlets=[Asset("mendoza_station_information")]
    )
    def fetch_station_information() -> list[dict]:
        """
        Descarga la lista de estaciones desde la API GBFS de Mendoza y retorna
        una lista de diccionarios simples con campos relevantes.
        """
        url = "https://api.mendoza.smod.io/v1/gbfs/station_information.json"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        stations = payload.get("data", {}).get("stations", [])

        normalized = []
        for station in stations:
            rental_uris = station.get("rental_uris", {}) or {}
            normalized.append(
                {
                    "station_id": station.get("station_id"),
                    "name": station.get("name"),
                    "lat": station.get("lat"),
                    "lon": station.get("lon"),
                    "capacity": station.get("capacity"),
                    "android_uri": rental_uris.get("android"),
                    "ios_uri": rental_uris.get("ios"),
                }
            )

        return normalized

    @task
    def write_csv(stations: list[dict]) -> str:
        """
        Escribe las estaciones en CSV bajo include/output/stations.csv.
        Retorna la ruta escrita.
        """
        # Ruta: <repo_root>/include/output
        repo_root = Path(__file__).resolve().parent.parent
        output_dir = repo_root / "include" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "stations.csv"

        fieldnames = [
            "station_id",
            "name",
            "lat",
            "lon",
            "capacity",
            "android_uri",
            "ios_uri",
        ]

        with output_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in stations:
                writer.writerow({key: row.get(key) for key in fieldnames})

        print(f"CSV escrito en: {output_path}")
        return str(output_path)

    @task(
        outlets=[Asset("mendoza_trips_data")]
    )
    def fetch_trips_data() -> str:
        """
        Descarga datos de viajes desde la API de Mendoza por períodos de 3 meses y los combina en un CSV.
        Retorna la ruta del archivo CSV generado.
        """
        # Configuración de fechas (último año dividido en períodos de 3 meses)
        end_date = today()
        start_date = end_date.subtract(years=1)
        
        # URL del endpoint de exportación
        base_url = "https://api.mendoza.smod.io/v1/trip/exportcsv"
        
        # Headers necesarios
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "es-419,es;q=0.8",
            "origin": "https://app.mendoza.smod.io",
            "referer": "https://app.mendoza.smod.io/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Token JWT usando Variable de Airflow
        try:
            jwt_token = Variable.get("MENDOZA_JWT_TOKEN")
            print(f"Token JWT encontrado: Sí")
            print(f"Token configurado en headers. Longitud: {len(jwt_token)} caracteres")
            headers["authorization"] = jwt_token
        except Exception as e:
            print(f"ERROR: No se pudo obtener la variable MENDOZA_JWT_TOKEN: {e}")
            print("Asegúrate de que la variable esté configurada en Admin → Variables")
            raise ValueError(f"MENDOZA_JWT_TOKEN no está configurado: {e}")
        
        # Crear directorio de salida
        repo_root = Path(__file__).resolve().parent.parent
        output_dir = repo_root / "include" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Archivo final combinado
        filename = f"trips_{start_date.format('YYYY-MM-DD')}_to_{end_date.format('YYYY-MM-DD')}.csv"
        output_path = output_dir / filename
        
        all_data = []
        header_written = False
        
        # Dividir el año en períodos de 3 meses
        current_start = start_date
        period_count = 0
        
        while current_start < end_date:
            period_count += 1
            current_end = min(current_start.add(months=3), end_date)
            
            print(f"\n--- Período {period_count}: {current_start.format('YYYY-MM-DD')} a {current_end.format('YYYY-MM-DD')} ---")
            
            params = {
                "order": "id,DESC",
                "scope": "status", 
                "fromTime": current_start.format("YYYY-MM-DD"),
                "toTime": current_end.format("YYYY-MM-DD")
            }
            
            try:
                print(f"Descargando datos del período {period_count}...")
                response = requests.get(base_url, params=params, headers=headers, timeout=120)  # 2 minutos por período
                print(f"Status code: {response.status_code}")
                
                response.raise_for_status()
                
                # Procesar datos del período
                lines = response.text.strip().split('\n')
                if lines and lines[0]:  # Si hay datos
                    if not header_written:
                        all_data.append(lines[0])  # Header solo una vez
                        header_written = True
                    
                    # Agregar datos (saltando el header)
                    all_data.extend(lines[1:])
                    print(f"Datos del período {period_count}: {len(lines)-1} registros")
                else:
                    print(f"Período {period_count}: Sin datos")
                
            except requests.exceptions.RequestException as e:
                print(f"Error en período {period_count}: {e}")
                print(f"Continuando con el siguiente período...")
                continue
            
            # Mover al siguiente período
            current_start = current_end
        
        # Guardar todos los datos combinados
        try:
            with output_path.open(mode="w", newline="", encoding="utf-8") as f:
                f.write('\n'.join(all_data))
            
            file_size = output_path.stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            total_records = len(all_data) - 1 if header_written else 0
            
            print(f"\n=== RESUMEN ===")
            print(f"CSV de viajes descargado en: {output_path}")
            print(f"Tamaño del archivo: {file_size:,} bytes ({file_size_mb:.2f} MB)")
            print(f"Total de registros: {total_records:,}")
            print(f"Períodos procesados: {period_count}")
            print(f"Rango de datos: {start_date.format('YYYY-MM-DD')} a {end_date.format('YYYY-MM-DD')}")
            
            return str(output_path)
            
        except Exception as e:
            print(f"Error al guardar archivo final: {e}")
            raise

    # Ejecutar ambas tareas
    write_csv(fetch_station_information())
    fetch_trips_data()


# Instantiate the DAG
mendoza_stations()
