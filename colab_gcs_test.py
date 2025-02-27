from IPython.display import display
from google.cloud import storage
from google.colab import files
from google.auth import default
from google.colab import auth
import ipywidgets as widgets
import gspread
import pandas as pd
import os

files.upload()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "daap-452113-7f0c12bb5ab9.json"

def connect_storage():
    """Conectar con Google Cloud Storage"""
    return storage.Client()

def list_files_in_folder(client, bucket_name, folder):
    """Listar archivos CSV o Parquet dentro de una carpeta específica en un bucket de GCS"""
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder + '/')
    return [blob.name for blob in blobs if blob.name.endswith('.csv') or blob.name.endswith('.parquet')]

def download_and_merge_files(client, bucket_name, folder):
    """Descargar y combinar todos los archivos CSV y Parquet de una carpeta en un DataFrame"""
    bucket = client.get_bucket(bucket_name)
    blobs = list_files_in_folder(client, bucket_name, folder)
    dataframes = []
    
    for blob_name in blobs:
        blob = bucket.blob(blob_name)
        local_filename = os.path.basename(blob_name)
        blob.download_to_filename(local_filename)
        
        if blob_name.endswith('.parquet'):
            df = pd.read_parquet(local_filename)
        else:
            df = pd.read_csv(local_filename)
        
        dataframes.append(df)
        os.remove(local_filename)  # Eliminar archivo después de la carga
    
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        return None

def create_ui(client, bucket_name, folder_name):
    """Crear la interfaz para filtrar y exportar datos"""
    global df_global
    df = download_and_merge_files(client, bucket_name, folder_name)
    
    if df is None:
        print("No se encontraron archivos CSV o Parquet en la carpeta especificada.")
        return
    
    country_widget = widgets.Dropdown(
        options=['MEX', 'CHL', 'PER', 'COL', 'General'],
        description='País:',
        style={'description_width': 'initial'}
    )
    
    columns_widget = widgets.SelectMultiple(
        options=df.columns,
        description='Campos:',
        style={'description_width': 'initial'}
    )
    
    action_widget = widgets.RadioButtons(
        options=['Descargar CSV', 'Cargar en DataFrame'],
        description='Acción:',
        style={'description_width': 'initial'}
    )
    
    button = widgets.Button(description="Ejecutar")
    output = widgets.Output()
    
    def on_button_clicked(b):
        global df_global
        with output:
            output.clear_output()
            selected_country = country_widget.value
            selected_columns = list(columns_widget.value)
            
            if selected_country == "General":
                df_filtered = df[selected_columns]
            else:
                df_filtered = df[df['shipments_origin_country'] == selected_country][selected_columns]
            
            if action_widget.value == 'Descargar CSV':
                csv_filename = folder_name + '_filtered.csv'
                df_filtered.to_csv(csv_filename, index=False)
                print(f"Tamaño del archivo: {os.path.getsize(csv_filename) / (1024 * 1024):.2f} MB")
                files.download(csv_filename)
                print(f"Archivo {csv_filename} listo para descargar en formato CSV.")
            elif action_widget.value == 'Cargar en DataFrame':
                df_global = df_filtered
                display(df_global.info())
                display(df_global.head())
                print("El DataFrame ha sido cargado y está disponible como 'df_global'.")
    
    button.on_click(on_button_clicked)
    
    display(widgets.Label("Selecciona el país para filtrar:"), country_widget,
            widgets.Label("Selecciona los campos de tu reporte:"), columns_widget,
            widgets.Label("Selecciona la acción a realizar:"), action_widget,
            button, output)

def main():
    client = connect_storage()
    create_ui(client, BUCKET_NAME, FOLDER_NAME)

if __name__ == "__main__":
    main()