# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ============================================================
# ELECTROCASA - 00_SETUP
# Configuración inicial del Lakehouse
# ============================================================

# El entorno dev usa el catálogo electrocasa, donde están los datos
# y al que apunta actualmente el pipeline.
CATALOGS = {
    "dev": "electrocasa",
    "prod": "electrocasa_prod"
}

SCHEMAS = ["bronze", "silver", "gold"]
VOLUME_NAME = "vol_landing"

print("Configuración inicial de ElectroCasa")
print(f"Entornos: {CATALOGS}")
print(f"Schemas: {SCHEMAS}")
print(f"Volume: {VOLUME_NAME}")

# COMMAND ----------

# 02. CREACIÓN DE SCHEMAS Y VOLUMES
for entorno, catalogo in CATALOGS.items():
    print(f"\nConfigurando entorno: {entorno.upper()} ({catalogo})")

    for schema in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema}")
        print(f"Schema verificado: {catalogo}.{schema}")

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.default.{VOLUME_NAME}")
    print(f"Volume verificado: {catalogo}.default.{VOLUME_NAME}")

print("Configuración de schemas y volumes finalizada.")

# COMMAND ----------

# 03. CONFIGURACIÓN DE PERMISOS UNITY CATALOG
# Los grupos deben existir previamente en el workspace.
GRANTS = {
    "bronze": {
        "electrocasa_ingenieria": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]
    },
    "silver": {
        "electrocasa_ingenieria": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]
    },
    "gold": {
        "electrocasa_ingenieria": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"],
        "electrocasa_analistas": ["USE SCHEMA", "SELECT"],
        "electrocasa_auditoria": ["USE SCHEMA", "SELECT"]
    }
}

for entorno, catalogo in CATALOGS.items():
    print(f"\nConfigurando permisos: {entorno.upper()} ({catalogo})")

    for grupo in ["electrocasa_ingenieria", "electrocasa_analistas", "electrocasa_auditoria"]:
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{grupo}`")

    for schema, grupos in GRANTS.items():
        for grupo, privilegios in grupos.items():
            spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalogo}.{schema} TO `{grupo}`")

            for privilegio in privilegios:
                if privilegio != "USE SCHEMA":
                    spark.sql(
                        f"GRANT {privilegio} ON SCHEMA {catalogo}.{schema} TO `{grupo}`"
                    )
            print(f"{grupo}: {catalogo}.{schema} -> {privilegios}")

print("Permisos configurados correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS electrocasa.bronze.catalogo_raw (
# MAGIC     producto_id STRING,
# MAGIC     nombre_producto STRING,
# MAGIC     categoria STRING,
# MAGIC     marca STRING,
# MAGIC     precio_lista STRING,
# MAGIC     activo STRING,
# MAGIC     _source_file STRING,
# MAGIC     _source_file_name STRING,
# MAGIC     _source_system STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     ingestion_batch_id STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS electrocasa.bronze.sucursales_raw (
# MAGIC     sucursal_id STRING,
# MAGIC     nombre_sucursal STRING,
# MAGIC     distrito STRING,
# MAGIC     ciudad STRING,
# MAGIC     region STRING,
# MAGIC     fecha_apertura STRING,
# MAGIC     _source_file STRING,
# MAGIC     _source_file_name STRING,
# MAGIC     _source_system STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     ingestion_batch_id STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# 05. COPY INTO - CATÁLOGO (idempotente por archivo)
spark.sql("""
COPY INTO electrocasa.bronze.catalogo_raw
FROM (
    SELECT
        CAST(producto_id AS STRING) AS producto_id,
        CAST(nombre_producto AS STRING) AS nombre_producto,
        CAST(categoria AS STRING) AS categoria,
        CAST(marca AS STRING) AS marca,
        CAST(precio_lista AS STRING) AS precio_lista,
        CAST(activo AS STRING) AS activo,
        CAST(_metadata.file_path AS STRING) AS _source_file,
        CAST(_metadata.file_name AS STRING) AS _source_file_name,
        'catalogo' AS _source_system,
        current_timestamp() AS ingestion_timestamp,
        sha2(_metadata.file_path, 256) AS ingestion_batch_id
    FROM '/Volumes/electrocasa/default/vol_landing/catalogo/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true')
""")
print("COPY INTO catálogo ejecutado.")

# COMMAND ----------

# 06. COPY INTO - SUCURSALES (idempotente por archivo)
spark.sql("""
COPY INTO electrocasa.bronze.sucursales_raw
FROM (
    SELECT
        CAST(sucursal_id AS STRING) AS sucursal_id,
        CAST(nombre_sucursal AS STRING) AS nombre_sucursal,
        CAST(distrito AS STRING) AS distrito,
        CAST(ciudad AS STRING) AS ciudad,
        CAST(region AS STRING) AS region,
        CAST(fecha_apertura AS STRING) AS fecha_apertura,
        CAST(_metadata.file_path AS STRING) AS _source_file,
        CAST(_metadata.file_name AS STRING) AS _source_file_name,
        'sucursales' AS _source_system,
        current_timestamp() AS ingestion_timestamp,
        sha2(_metadata.file_path, 256) AS ingestion_batch_id
    FROM '/Volumes/electrocasa/default/vol_landing/sucursales/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
""")
print("COPY INTO sucursales ejecutado.")

# COMMAND ----------

# 07. VALIDACIÓN DE CARGAS COPY INTO
for tabla in ["catalogo_raw", "sucursales_raw"]:
    print(f"{tabla}: {spark.table(f'electrocasa.bronze.{tabla}').count()} registros")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'catalogo_raw' AS tabla, COUNT(*) AS registros
# MAGIC FROM electrocasa.bronze.catalogo_raw
# MAGIC UNION ALL
# MAGIC SELECT 'sucursales_raw', COUNT(*)
# MAGIC FROM electrocasa.bronze.sucursales_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _source_file_name, COUNT(*) AS registros
# MAGIC FROM electrocasa.bronze.catalogo_raw
# MAGIC GROUP BY _source_file_name
# MAGIC ORDER BY _source_file_name;

# COMMAND ----------

# 08. INVENTARIO DE ARCHIVOS DE LANDING
for carpeta in ["ventas", "empleados", "devoluciones", "resenas"]:
    ruta = f"/Volumes/electrocasa/default/vol_landing/{carpeta}/"
    print(f"\n{carpeta.upper()}")
    for archivo in dbutils.fs.ls(ruta):
        print(archivo.name, archivo.size)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION electrocasa.silver.mask_dni(value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC     WHEN is_account_group_member('electrocasa_ingenieria') THEN value
# MAGIC     ELSE concat('******', right(value, 2))
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION electrocasa.silver.mask_salario(value DECIMAL(12,2))
# MAGIC RETURNS DECIMAL(12,2)
# MAGIC RETURN CASE
# MAGIC     WHEN is_account_group_member('electrocasa_ingenieria') THEN value
# MAGIC     ELSE CAST(NULL AS DECIMAL(12,2))
# MAGIC END;