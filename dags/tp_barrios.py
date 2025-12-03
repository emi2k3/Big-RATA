from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, count_if, when, trim, upper
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

# Sesion spark
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

# ============ NUEVA ARQUITECTURA DE DATA LAKE ============
BASE_DIR = '/opt/airflow'

# Zonas del Data Lake
LANDING_ZONE = os.path.join(BASE_DIR, 'datalake', 'landing')
RAW_ZONE = os.path.join(BASE_DIR, 'datalake', 'raw')
REFINED_ZONE = os.path.join(BASE_DIR, 'datalake', 'refined')
TEMP_ZONE = os.path.join(BASE_DIR, 'datalake', 'temp')

# Archivo de entrada en la zona LANDING
INPUT_FILE = os.path.join(LANDING_ZONE, 'otros-delitos.csv')

# Rutas específicas para este pipeline
RAW_DELITOS_ORIGINAL = os.path.join(RAW_ZONE, 'delitos_original.parquet')  # Datos sin limpiar
RAW_DELITOS_CLEAN = os.path.join(RAW_ZONE, 'delitos_clean.parquet')        # Datos limpios
REFINED_DELITOS_PATH = os.path.join(REFINED_ZONE, 'delitos_agregados.parquet')
OUTPUT_CSV = os.path.join(BASE_DIR, 'output', 'csv_output')
# ====================================================


def extract_data(**context):
    """
    LANDING -> RAW
    Lee el archivo CSV de entrada y lo guarda en formato Parquet en la zona RAW
    """
    spark = get_spark_session()

    print(f"Extrayendo datos desde: {INPUT_FILE}")
    
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

    # Guardar en zona RAW (datos originales sin procesar)
    print(f"Guardando datos originales en zona RAW: {RAW_DELITOS_ORIGINAL}")
    df.write.mode("overwrite").parquet(RAW_DELITOS_ORIGINAL)
    
    # Pasar la ruta a la siguiente tarea
    context['ti'].xcom_push(key='ruta_datos_raw', value=RAW_DELITOS_ORIGINAL)

    print(f"Extraccion completada. Registros: {df.count()}")
    return RAW_DELITOS_ORIGINAL

def clean_and_validate_data(**context):
    """
    RAW -> RAW (cleaned)
    Limpia y valida la calidad de los datos:
    - Elimina duplicados
    - Maneja valores nulos
    - Valida formatos de datos
    - Estandariza valores
    """
    spark = get_spark_session()
    
    # Leer desde zona RAW
    raw_path = context['ti'].xcom_pull(task_ids='extract', key='ruta_datos_raw')
    print(f"Leyendo datos desde zona RAW: {raw_path}")
    
    df = spark.read.parquet(raw_path)
    
    registros_iniciales = df.count()
    print(f"Registros iniciales: {registros_iniciales}")
    
    # ============ 1. ELIMINAR DUPLICADOS ============
    print("Eliminando duplicados...")
    registros_antes = df.count()
    
    # Eliminar duplicados exactos basados en ID_EVENTO si existe, sino todos los campos
    if 'ID_EVENTO' in df.columns:
        df = df.dropDuplicates(['ID_EVENTO'])
    else:
        df = df.dropDuplicates()
    
    duplicados_eliminados = registros_antes - df.count()
    print(f"   Duplicados eliminados: {duplicados_eliminados}")
    
    # ============ 2. LIMPIAR VALORES INVÁLIDOS DE TEXTO ============
    print("Limpiando valores de texto inválidos...")
    
    # Lista de valores que se consideran "sin dato"
    valores_sin_dato = ['SIN DATO', 'SIN DATOS', 'N/A', 'NA', 'NULL', 'NONE', '', ' ']
    
    # Reemplazar estos valores por NULL en todas las columnas
    for columna in df.columns:
        df = df.withColumn(columna, 
            when(upper(trim(col(columna))).isin(valores_sin_dato), None)
            .otherwise(col(columna))
        )
    
    # ============ 3. VALIDAR Y LIMPIAR VALORES NULOS ============
    print("Validando valores nulos...")
    
    # Contar nulos por columna
    print("   Nulos por columna:")
    for columna in df.columns:
        nulos = df.filter(col(columna).isNull()).count()
        if nulos > 0:
            print(f"   - {columna}: {nulos} nulos ({(nulos/registros_iniciales)*100:.2f}%)")
    
    # Eliminar registros donde campos críticos sean nulos
    campos_criticos = ['DELITO', 'AÑO', 'MES', 'BARRIO_MONTEVIDEO', 'JURISDICCION']
    
    for campo in campos_criticos:
        if campo in df.columns:
            registros_antes = df.count()
            df = df.filter(col(campo).isNotNull())
            eliminados = registros_antes - df.count()
            if eliminados > 0:
                print(f"   Eliminados {eliminados} registros con {campo} nulo o 'SIN DATO'")
    
    # Para campos no críticos, rellenar con valores por defecto
    if 'DIA_SEMANA' in df.columns:
        df = df.fillna({'DIA_SEMANA': 'DESCONOCIDO'})
    
    if 'TENTATIVA' in df.columns:
        df = df.fillna({'TENTATIVA': 'NO'})
    
    # ============ 4. ESTANDARIZAR VALORES ============
    print("Estandarizando valores...")
    
    # Convertir texto a mayúsculas y quitar espacios extra
    columnas_texto = ['DELITO', 'BARRIO_MONTEVIDEO', 'JURISDICCION', 'DEPTO', 
                      'DIA_SEMANA', 'TENTATIVA', 'VICT_RAP', 'VICT_HUR', 'MES']
    
    for columna in columnas_texto:
        if columna in df.columns:
            df = df.withColumn(columna, upper(trim(col(columna))))
    
    # Normalizar valores de TENTATIVA (SI/NO en mayúsculas)
    if 'TENTATIVA' in df.columns:
        df = df.withColumn('TENTATIVA', 
            when(upper(col('TENTATIVA')).isin(['SI', 'S', 'SÍ', '1', 'TRUE']), 'SI')
            .when(upper(col('TENTATIVA')).isin(['NO', 'N', '0', 'FALSE']), 'NO')
            .otherwise('NO')
        )
    
    # ============ 5. VALIDAR RANGOS DE DATOS ============
    print("Validando rangos de datos...")
    
    # Validar años razonables (últimos 20 años)
    año_actual = datetime.now().year
    if 'AÑO' in df.columns:
        registros_antes = df.count()
        df = df.filter(
            (col('AÑO') >= año_actual - 20) & 
            (col('AÑO') <= año_actual)
        )
        eliminados = registros_antes - df.count()
        if eliminados > 0:
            print(f"   Eliminados {eliminados} registros con años fuera de rango")
    
    # Validar meses (lista de meses válidos en español)
    if 'MES' in df.columns:
        meses_validos = [
            'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO',
            'JULIO', 'AGOSTO', 'SETIEMBRE', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'
        ]
        
        registros_antes = df.count()
        df = df.filter(upper(col('MES')).isin(meses_validos))
        eliminados = registros_antes - df.count()
        if eliminados > 0:
            print(f"   Eliminados {eliminados} registros con meses inválidos")
    
    # Validar horas (0-23) si existe
    if 'HORA' in df.columns:
        registros_antes = df.count()
        df = df.filter((col('HORA') >= 0) & (col('HORA') <= 23))
        eliminados = registros_antes - df.count()
        if eliminados > 0:
            print(f"   Eliminados {eliminados} registros con horas inválidas")
    
    # ============ 6. CREAR COLUMNAS DERIVADAS ============
    print("Creando columnas derivadas...")
    
    # Franja horaria (si existe HORA)
    if 'HORA' in df.columns:
        df = df.withColumn('FRANJA_HORARIA',
            when((col('HORA') >= 6) & (col('HORA') < 12), 'MAÑANA')
            .when((col('HORA') >= 12) & (col('HORA') < 18), 'TARDE')
            .when((col('HORA') >= 18) & (col('HORA') < 24), 'NOCHE')
            .otherwise('MADRUGADA')
        )
    
    # ============ GUARDAR DATOS LIMPIOS EN NUEVA RUTA ============
    print(f"Guardando datos limpios en zona RAW (clean): {RAW_DELITOS_CLEAN}")
    df.write.mode("overwrite").parquet(RAW_DELITOS_CLEAN)
    
    # Pasar la ruta a la siguiente tarea
    context['ti'].xcom_push(key='ruta_datos_limpios', value=RAW_DELITOS_CLEAN)
    
    registros_finales = df.count()
    total_eliminados = registros_iniciales - registros_finales
    porcentaje_eliminado = (total_eliminados / registros_iniciales) * 100
    
    print(f"\nResumen de limpieza:")
    print(f"   - Registros iniciales: {registros_iniciales:,}")
    print(f"   - Registros finales: {registros_finales:,}")
    print(f"   - Total eliminados: {total_eliminados:,} ({porcentaje_eliminado:.2f}%)")
    
    # Mostrar muestra de datos limpios
    print("\nMuestra de datos limpios:")
    if 'HORA' in df.columns and 'FRANJA_HORARIA' in df.columns:
        df.select('AÑO', 'MES', 'HORA', 'FRANJA_HORARIA', 'DELITO', 'BARRIO_MONTEVIDEO').show(5, truncate=False)
    else:
        df.select('AÑO', 'MES', 'DELITO', 'BARRIO_MONTEVIDEO').show(5, truncate=False)
    
    return RAW_DELITOS_CLEAN


def transform_with_pyspark(**context):
    """
    RAW -> REFINED
    Tarea de transformación usando PySpark
    """
    spark = get_spark_session()
    
    # Leer desde zona RAW (datos limpios)
    raw_path = context['ti'].xcom_pull(task_ids='clean_validate', key='ruta_datos_limpios')
    print(f"Leyendo datos limpios desde zona RAW: {raw_path}")
    
    df = spark.read.parquet(raw_path)

    # Realizar transformaciones
    print("Aplicando transformaciones...")
    
    # Columnas de agrupación - DIMENSIONES BÁSICAS
    group_columns = ["BARRIO_MONTEVIDEO", "MES", "AÑO", "DELITO", "JURISDICCION"]
    
    # Agregar dimensiones adicionales si existen
    if 'DIA_SEMANA' in df.columns:
        group_columns.append("DIA_SEMANA")
    if 'FRANJA_HORARIA' in df.columns:
        group_columns.append("FRANJA_HORARIA")
    if 'DEPTO' in df.columns:
        group_columns.append("DEPTO")
    if 'VICT_RAP' in df.columns:
        group_columns.append("VICT_RAP")
    if 'VICT_HUR' in df.columns:
        group_columns.append("VICT_HUR")
    
    # Agregar métricas básicas y adicionales
    df_transformed = df.groupBy(*group_columns).agg(
        # MÉTRICAS BÁSICAS
        count_if(col("TENTATIVA") == "SI").alias("Tentativa_SI"),
        count_if(col("TENTATIVA") == "NO").alias("Tentativa_NO"),
        count("*").alias("Cantidad_Crimenes"),
    )

    # Guardar en zona REFINED (datos procesados listos para análisis)
    print(f"Guardando datos en zona REFINED: {REFINED_DELITOS_PATH}")
    df_transformed.write.mode("overwrite").parquet(REFINED_DELITOS_PATH)
    
    print("Datos transformados (primeras 20 filas):")
    df_transformed.show()
    
    # Pasar la ruta a la siguiente tarea
    context['ti'].xcom_push(key='ruta_datos_transformados', value=REFINED_DELITOS_PATH)
    
    print(f"Transformación completada. Registros agregados: {df_transformed.count()}")
    return REFINED_DELITOS_PATH
        
        

def load_data(**context):
    """
    REFINED -> OUTPUT
    Carga los datos transformados desde la zona REFINED al directorio de salida
    """
    spark = get_spark_session()
    
    # Leer desde zona REFINED
    refined_path = context['ti'].xcom_pull(task_ids='transform', key='ruta_datos_transformados')
    print(f"Leyendo datos desde zona REFINED: {refined_path}")
    
    df_transformed = spark.read.parquet(refined_path)
    
    # Exportar a CSV para consumo externo
    print(f"Exportando a CSV: {OUTPUT_CSV}")
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    
    df_transformed.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(OUTPUT_CSV)
    
    spark.stop()
    print(f"Datos cargados exitosamente en: {OUTPUT_CSV}")

def cleanup_parquet_files(**context):
    """
    Limpia archivos temporales (opcional: puedes mantener RAW y REFINED para análisis)
    """
    import shutil
    
    # Solo limpiamos la zona TEMP si existe
    if os.path.exists(TEMP_ZONE):
        print(f"Limpiando zona temporal: {TEMP_ZONE}")
        shutil.rmtree(TEMP_ZONE)
        os.makedirs(TEMP_ZONE, exist_ok=True)
    
    print("Limpieza completada")


# Definir el DAG
with DAG(
    'pyspark_etl_barrios',
    default_args=default_args,
    description='''Pipeline ETL con arquitectura Data Lake (Landing/Raw/Refined).
    Incluye limpieza de datos, validación de calidad y transformaciones.
    Analiza delitos en Montevideo con múltiples métricas y dimensiones.''',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pyspark', 'etl', 'datalake', 'delitos', 'data-quality'],
) as dag:
    
    # Tarea 1: Extracción (LANDING -> RAW)
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    # Tarea 2: Limpieza y Validación (RAW -> RAW cleaned)
    clean_validate_task = PythonOperator(
        task_id='clean_validate',
        python_callable=clean_and_validate_data,
    )

    # Tarea 3: Transformación (RAW -> REFINED)
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_with_pyspark,
    )
    
    # Tarea 4: Carga (REFINED -> OUTPUT)
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    # Tarea 5: Limpieza
    cleanup_task = PythonOperator(
        task_id='clean',
        python_callable=cleanup_parquet_files,
    )
    
    # Definir dependencias
    extract_task >> clean_validate_task >> transform_task >> load_task >> cleanup_task

