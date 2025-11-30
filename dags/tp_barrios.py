from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg,count, count_if

import os
# Configuración por defecto del DAG
default_args = {
    'owner': 'equipo2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Sesion spark
_spark_session = None

def get_spark_session():
    """Singleton para SparkSession"""
    global _spark_session
    if _spark_session is None:
        _spark_session = SparkSession.builder \
        .appName("ETL_Process") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()
    
    # Configurar permisos en Hadoop
    _spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.permissions.umask-mode", "000")
    
    return _spark_session

#Rutas de archivos

BASE_DIR = '/opt/airflow'  # Directorio base dentro del contenedor
INPUT_FILE = os.path.join(BASE_DIR, 'data', 'otros-delitos.csv')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'datos_filtrados.csv')


def extract_data(**context):
    """
    Lee el archivo CSV de entrada y guarda los datos en XCom
    """
    # Crear sesión de Spark     
    spark = get_spark_session()

    # Leer datos con separador correcto y codificación
    df = spark.read.csv(
        INPUT_FILE,
        header=True,
        sep=';',                 
        inferSchema=True,
        encoding='UTF-8',        
        quote='"',
        escape='\\'
    )

    # Limpiar nombres de columnas: quitar BOM, comillas y espacios extra
    clean_cols = [c.strip().strip('"').replace('\ufeff', '') for c in df.columns]
    df = df.toDF(*clean_cols)


    temp_path = "/opt/airflow/temp/datos_raw.parquet"
    df.write.mode("overwrite").parquet(temp_path)
    
    #Pasamos la ruta del archivo Parquet
    context['ti'].xcom_push(key='ruta_datos_raw', value=temp_path)

    return "Extracción completada"

def transform_with_pyspark(**context):
    """
    Tarea de transformación usando PySpark
    """
        # Obtener el data frrame

    spark = get_spark_session()
    temp_path = context['ti'].xcom_pull(task_ids='extract', key='ruta_datos_raw')
    df = spark.read.parquet(temp_path)

        # Realizar transformaciones
    df_transformed = df \
        .groupBy("BARRIO_MONTEVIDEO","MES","AÑO","DELITO","JURISDICCION").agg(
            count_if(col("TENTATIVA")=="SI").alias("Tentativa_SI"),
            count_if(col("TENTATIVA")=="NO").alias("Tentativa_NO"),
            count("*").alias("Cantidad_Crimenes"),
        )

    temp_path = "/opt/airflow/temp/datos_transformados.parquet"
    df_transformed.write.mode("overwrite").parquet(temp_path)
    print("Datos transformados:")
    df_transformed.show()
        
        # Guardar resultados
    context['ti'].xcom_push(key='ruta_datos_transformados', value=temp_path)
    print("Sesión de Spark cerrada")

        
    return OUTPUT_DIR
        
        

def load_data(**context):
    """
    Carga los datos transformados al outputdir
    """
    

    # Crear el directorio de salida
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    spark = get_spark_session()
    temp_path = context['ti'].xcom_pull(task_ids='transform', key='ruta_datos_transformados')
    df_transformed = spark.read.parquet(temp_path)
    
    output_path = os.path.join(OUTPUT_DIR, 'csv_output')

    df_transformed.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
    spark.stop()
    print(f"Datos guardados en: {output_path}")

def cleanup_parquet_files(**context):
    """
    Limpia todos los archivos temporales generados por los DAGS
    """
    import shutil
    spark = get_spark_session()
    path_raw = context['ti'].xcom_pull(task_ids='extract', key='ruta_datos_raw')
    path_transformed = context['ti'].xcom_pull(task_ids='transform', key='ruta_datos_transformados')

    for path in [path_raw,path_transformed]:
        if path and os.path.exists(path):
            shutil.rmtree(path)
    spark.stop()


# Definir el DAG
with DAG(
    'pyspark_etl_barrios',
    default_args=default_args,
    description='''Cuenta la catidad total de crimenes junto con las tentativas y no
    tentativas agrupado por tipo de crimen, mes, barrio, jurisdicción y tipo de victima. ''',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pyspark', 'etl', 'example'],
) as dag:
    
    # Tarea 1: Extracción
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )
    
    # Tarea 2: Transformación con PySpark
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_with_pyspark,
    )
    
    # Tarea 3: Carga
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    cleanup_task = PythonOperator(
        task_id='clean',
        python_callable=cleanup_parquet_files,
    )
    
    # Definir dependencias
    extract_task >> transform_task >> load_task >> cleanup_task

