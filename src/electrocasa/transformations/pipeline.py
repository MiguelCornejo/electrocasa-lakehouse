from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    ArrayType, TimestampType
)

# ElectroCasa - Lakeflow Spark Declarative Pipelines
# Las tablas Bronze de catalogo y sucursales se cargan con COPY INTO
# fuera del pipeline. COPY INTO debe guardar _source_file_name (o fecha_snapshot
# para catalogo), _source_system, ingestion_timestamp e ingestion_batch_id.
# tracking_envios llega mediante Lakeflow Connect; sus columnas de auditoria
# se completan aquí porque el conector no necesariamente entrega metadata de archivo.
CATALOG = spark.conf.get("catalog_name", "electrocasa")
LANDING = f"/Volumes/{CATALOG}/default/vol_landing"

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


# ------------------------------------------------------------------
# Esquemas explícitos de los CSV incrementales (sin inferSchema)
# ------------------------------------------------------------------
VENTAS_SCHEMA = StructType([
    StructField("venta_id", StringType()),
    StructField("sucursal_id", StringType()),
    StructField("producto_id", StringType()),
    StructField("vendedor_id", StringType()),
    StructField("cantidad", StringType()),
    StructField("monto_total", StringType()),
    StructField("metodo_pago", StringType()),
    StructField("fecha_venta", StringType()),
    StructField("canal", StringType()),
])

EMPLEADOS_SCHEMA = StructType([
    StructField("id_empleado", StringType()),
    StructField("nombre", StringType()),
    StructField("dni", StringType()),
    StructField("email", StringType()),
    StructField("salario", StringType()),
    StructField("sucursal_id", StringType()),
    StructField("cargo", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("fecha_evento", StringType()),
    StructField("fecha_registro", StringType()),
])

DEVOLUCIONES_SCHEMA = StructType([
    StructField("devolucion_id", StringType()),
    StructField("venta_id", StringType()),
    StructField("sucursal_id", StringType()),
    StructField("producto_id", StringType()),
    StructField("motivo", StringType()),
    StructField("monto_reembolso", StringType()),
    StructField("fecha_devolucion", StringType()),
])


def _metadata(df, source):
    """Añade auditoría de archivo, sistema, timestamp e identificador de lote."""
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_source_file_name", F.col("_metadata.file_name"))
          .withColumn("_source_system", F.lit(source))
          .withColumn(
              "ingestion_batch_id",
              F.sha2(
                  F.concat_ws(
                      "||",
                      F.col("_metadata.file_path"),
                      F.col("_metadata.file_modification_time").cast("string")
                  ),
                  256
              )
          )
    )


def _empty_to_null(column):
    return F.when(F.trim(F.col(column)) == "", F.lit(None)).otherwise(F.trim(F.col(column)))


def _date3(column):
    # try_to_date evita fallos ANSI cuando una fila usa otro formato.
    return F.coalesce(
        F.expr(f"try_to_date(`{column}`, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(`{column}`, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date(substring(`{column}`, 1, 10), 'yyyy-MM-dd')")
    )


def _decimal12(column, currency=False):
    """Convierte formatos 1.234,50 y 1,299.90 a DECIMAL(12,2) sin ANSI errors."""
    c = f"trim(cast(`{column}` as string))"
    if currency:
        c = f"regexp_replace({c}, '(?i)s/\\\\s*', '')"
    c = f"regexp_replace({c}, '[^0-9,.-]', '')"
    both = f"(instr({c}, ',') > 0 AND instr({c}, '.') > 0)"
    last_separator = f"regexp_extract({c}, '.*([,.])[^,.]*$', 1)"
    normalized = (
        f"CASE WHEN {both} AND {last_separator} = ',' "
        f"THEN regexp_replace(regexp_replace({c}, '\\\\.', ''), ',', '.') "
        f"WHEN {both} THEN regexp_replace({c}, ',', '') "
        f"WHEN instr({c}, ',') > 0 THEN regexp_replace({c}, ',', '.') "
        f"ELSE {c} END"
    )
    return F.expr(f"try_cast(nullif({normalized}, '') as decimal(12,2))")


def _col_or(df, name, data_type="string", default=None):
    if name in df.columns:
        return F.col(name)
    return F.lit(default).cast(data_type)


def _ensure_audit(df, source):
    out = (df
        .withColumn("ingestion_timestamp",
                    F.col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else F.current_timestamp())
        .withColumn("_source_file", F.col("_source_file") if "_source_file" in df.columns else F.lit(f"system:{source}"))
        .withColumn("_source_file_name", _col_or(df, "_source_file_name"))
        .withColumn("_source_system", F.col("_source_system") if "_source_system" in df.columns else F.lit(source))
    )
    if "ingestion_batch_id" not in out.columns:
        out = out.withColumn("ingestion_batch_id", F.sha2(
            F.concat_ws("||", F.lit(source), F.col("ingestion_timestamp").cast("string")), 256))
    return out


def _quarantine(df, source, key_col, rule_col, reason_col):
    """Proyección común; tolera fuentes de tabla (COPY/connector) sin metadata de archivo."""
    file_expr = F.col("_source_file") if "_source_file" in df.columns else F.lit(f"origen_sistema:{source}")
    batch_expr = F.col("ingestion_batch_id") if "ingestion_batch_id" in df.columns else F.lit(None).cast("string")
    return df.select(
        F.lit(source).alias("fuente"),
        F.col(key_col).cast("string").alias("registro_id"),
        file_expr.cast("string").alias("archivo_origen"),
        batch_expr.cast("string").alias("lote_ingesta"),
        F.lit(rule_col) if isinstance(rule_col, str) else rule_col.alias("regla"),
        reason_col.alias("motivo"),
        F.current_timestamp().alias("fecha_procesamiento")
    )


# ==================================================================
# BRONZE - AUTO LOADER
# ==================================================================

@dp.table(name=f"{CATALOG}.bronze.ventas_raw",
          comment="Ventas originales CSV recibidas mediante Auto Loader")
def bronze_ventas():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .schema(VENTAS_SCHEMA)
          .load(f"{LANDING}/ventas/"))
    return _metadata(df, "ventas")


@dp.table(name=f"{CATALOG}.bronze.empleados_raw",
          comment="Eventos laborales originales recibidos mediante Auto Loader")
def bronze_empleados():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .schema(EMPLEADOS_SCHEMA)
          .load(f"{LANDING}/empleados/"))
    return _metadata(df, "empleados")


@dp.table(name=f"{CATALOG}.bronze.devoluciones_raw",
          comment="Devoluciones originales recibidas mediante Auto Loader")
def bronze_devoluciones():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .schema(DEVOLUCIONES_SCHEMA)
          .load(f"{LANDING}/devoluciones/"))
    return _metadata(df, "devoluciones")


@dp.table(name=f"{CATALOG}.bronze.resenas_raw",
          comment="Reseñas JSON Lines originales recibidas mediante Auto Loader")
def bronze_resenas():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "text")
          .load(f"{LANDING}/resenas/"))
    return (
        df.withColumnRenamed("value", "raw_json")
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_source_file_name", F.col("_metadata.file_name"))
          .withColumn("_source_system", F.lit("resenas"))
          .withColumn(
              "ingestion_batch_id",
              F.sha2(
                  F.concat_ws(
                      "||",
                      F.col("_metadata.file_path"),
                      F.col("_metadata.file_modification_time").cast("string")
                  ), 256
              )
          )
    )


# ==================================================================
# SILVER - CATÁLOGO (Bronze previamente cargada por COPY INTO)
# ==================================================================

@dp.materialized_view(name=f"{CATALOG}.silver.catalogo",
                      comment="Snapshots históricos limpios del catálogo")
def silver_catalogo():
    raw = _ensure_audit(spark.read.table(f"{BRONZE}.catalogo_raw"), "catalogo")
    filename = F.coalesce(F.col("_source_file_name"), F.col("_source_file"), F.lit(""))
    snapshot_from_name = (F.when(filename.contains("2026-03-01"), F.lit("2026-03-01").cast("date"))
                          .when(filename.contains("2026-08-01"), F.lit("2026-08-01").cast("date")))
    snapshot = (F.col("fecha_snapshot").cast("date") if "fecha_snapshot" in raw.columns
                else snapshot_from_name)

    category = F.lower(F.regexp_replace(F.trim(F.col("categoria")), r"[\s_-]+", ""))
    category_norm = (
        F.when(category == "cocina", "Cocina")
         .when(category == "climatizacion", "Climatización")
         .when(category == "electronica", "Electrónica")
         .when(category == "entretenimiento", "Entretenimiento")
         .when(category == "lineablanca", "Línea blanca")
    )
    price_text = F.regexp_replace(F.regexp_replace(F.col("precio_lista").cast("string"), r"(?i)s/\s*", ""), r"\s", "")
    price_text = F.when(
        price_text.contains(",") & price_text.contains("."),
        F.regexp_replace(F.regexp_replace(price_text, r"\.", ""), ",", ".")
    ).when(price_text.contains(","), F.regexp_replace(price_text, ",", ".")).otherwise(price_text)

    parsed = (raw
        .withColumn("fecha_snapshot", snapshot)
        .withColumn("producto_id", _empty_to_null("producto_id"))
        .withColumn("nombre_producto", _empty_to_null("nombre_producto"))
        .withColumn("categoria", category_norm)
        .withColumn("marca", _empty_to_null("marca"))
        .withColumn("precio_lista", _decimal12("precio_lista", currency=True))
        .withColumn("activo", F.lower(F.col("activo").cast("string")).isin("true", "1", "si", "sí"))
    )

    w = Window.partitionBy("producto_id", "fecha_snapshot")
    return (parsed
        .withColumn("_rows_per_key", F.count("*").over(w))
        .filter(
            F.col("producto_id").isNotNull()
            & F.col("nombre_producto").isNotNull()
            & F.col("fecha_snapshot").isNotNull()
            & F.col("precio_lista").isNotNull()
            & (F.col("precio_lista") > 0)
            & F.col("categoria").isNotNull()
            & (F.col("_rows_per_key") == 1)
        )
        .drop("_rows_per_key")
        .select(
            "producto_id", "nombre_producto", "categoria", "marca",
            "precio_lista", "activo", "fecha_snapshot",
            "_source_file", "_source_file_name", "_source_system",
            "ingestion_timestamp", "ingestion_batch_id"
        )
    )


@dp.materialized_view(name=f"{SILVER}.cuarentena_catalogo",
                      comment="Productos rechazados por precio, categoría, llave o snapshot")
def cuarentena_catalogo():
    raw = _ensure_audit(spark.read.table(f"{BRONZE}.catalogo_raw"), "catalogo")
    filename = F.coalesce(F.col("_source_file_name"), F.col("_source_file"), F.lit(""))
    snapshot = F.when(filename.contains("2026-03-01"), F.lit("2026-03-01").cast("date")) \
                .when(filename.contains("2026-08-01"), F.lit("2026-08-01").cast("date"))
    price_text = F.regexp_replace(F.regexp_replace(F.col("precio_lista").cast("string"), r"(?i)s/\s*", ""), r"\s", "")
    price_text = F.when(price_text.contains(",") & price_text.contains("."), F.regexp_replace(F.regexp_replace(price_text, r"\.", ""), ",", ".")) \
                   .when(price_text.contains(","), F.regexp_replace(price_text, ",", ".")).otherwise(price_text)
    p = _decimal12("precio_lista", currency=True)
    checked = (raw.withColumn("fecha_snapshot", snapshot).withColumn("_precio", p)
        .withColumn("_dup_count", F.count("*").over(Window.partitionBy("producto_id", "fecha_snapshot"))))
    reason = F.concat_ws("; ",
        F.when(F.col("_precio").isNull() | (F.col("_precio") <= 0), "precio nulo, no numérico o no positivo"),
        F.when(F.col("producto_id").isNull(), "producto_id nulo"),
        F.when(F.col("fecha_snapshot").isNull(), "snapshot no reconocido"),
        F.when(F.col("_dup_count") > 1, "producto duplicado dentro del snapshot; no se elige precio arbitrariamente"),
        F.when(F.col("categoria").isNull(), "categoría no reconocida")
    )
    bad = checked.withColumn("_reason", reason).filter(F.length("_reason") > 0)
    return _quarantine(bad, "catalogo", "producto_id", F.lit("reglas_catalogo"), F.col("_reason"))


# ==================================================================
# SILVER - SUCURSALES (Bronze previamente cargada por COPY INTO)
# ==================================================================

@dp.materialized_view(name=f"{SILVER}.sucursales",
                      comment="Maestro de sucursales normalizado")
def silver_sucursales():
    raw = _ensure_audit(spark.read.table(f"{BRONZE}.sucursales_raw"), "sucursales")
    cleaned = (raw
        .withColumn("sucursal_id", _empty_to_null("sucursal_id"))
        .withColumn("nombre_sucursal", F.trim("nombre_sucursal"))
        .withColumn("distrito", F.trim("distrito"))
        .withColumn("ciudad", F.initcap(F.trim("ciudad")))
        .withColumn("region", F.initcap(F.trim("region")))
        .withColumn("fecha_apertura", _date3("fecha_apertura"))
        .withColumn("_n", F.count("*").over(Window.partitionBy("sucursal_id")))
    )
    return (cleaned.filter(
        F.col("sucursal_id").isNotNull() & (F.col("_n") == 1)
        & F.col("nombre_sucursal").isNotNull() & F.col("ciudad").isNotNull()
        & F.col("fecha_apertura").isNotNull()
    ).drop("_n"))


@dp.materialized_view(name=f"{SILVER}.cuarentena_sucursales",
                      comment="Sucursales con llave duplicada o atributos obligatorios ausentes")
def cuarentena_sucursales():
    raw = _ensure_audit(spark.read.table(f"{BRONZE}.sucursales_raw"), "sucursales")
    d = raw.withColumn("_n", F.count("*").over(Window.partitionBy("sucursal_id")))
    bad = d.filter(F.col("sucursal_id").isNull() | (F.col("_n") > 1) | F.col("ciudad").isNull())
    return _quarantine(bad, "sucursales", "sucursal_id",
                       F.lit("sucursal_id_unico_o_ciudad"), F.lit("Llave nula/duplicada o ciudad ausente"))


# ==================================================================
# SILVER - VENTAS
# ==================================================================

def _ventas_normalizadas():
    raw = spark.read.table(f"{BRONZE}.ventas_raw")
    payment = F.upper(F.regexp_replace(F.trim(F.col("metodo_pago")), r"[^A-Za-zÁÉÍÓÚÜÑ]", ""))
    payment_norm = (
        F.when(payment == "PLIN", "PLIN")
         .when(payment == "YAPE", "YAPE")
         .when(payment.isin("TRANSFERENCIA", "TRF"), "TRANSFERENCIA")
         .when(payment.isin("EFECTIVO", "EFV"), "EFECTIVO")
         .when(payment.isin("TARJETA", "TC", "TD", "TARJETADECREDITO"), "TARJETA")
    )
    channel = F.lower(F.regexp_replace(F.trim(F.col("canal")), r"[\s-]+", "_"))
    channel = (F.when(channel.isin("tienda_fisica", "tienda", "fisica"), "tienda_fisica")
                 .when(channel.isin("ecommerce", "e_commerce", "online", "comercio_electronico"), "ecommerce"))
    return (raw
        .withColumn("venta_id", _empty_to_null("venta_id"))
        .withColumn("sucursal_id", F.coalesce(_empty_to_null("sucursal_id"), F.lit("SIN_ASIGNAR")))
        .withColumn("producto_id", _empty_to_null("producto_id"))
        .withColumn("vendedor_id", _empty_to_null("vendedor_id"))
        .withColumn("canal", channel)
        .withColumn("cantidad", F.expr("try_cast(cantidad as int)"))
        .withColumn("monto_total", _decimal12("monto_total"))
        .withColumn("fecha_venta", _date3("fecha_venta"))
        .withColumn("metodo_pago", payment_norm)
    )


@dp.materialized_view(name=f"{SILVER}.cuarentena_ventas",
                      comment="Ventas rechazadas por reglas críticas, referencias y duplicados")
def cuarentena_ventas():
    v = _ventas_normalizadas()
    catalog = spark.read.table(f"{SILVER}.catalogo").select("producto_id").distinct()
    joined = v.join(catalog.withColumn("_producto_ok", F.lit(1)), "producto_id", "left")
    duplicate_window = Window.partitionBy("venta_id").orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(), F.col("_source_file_name").asc_nulls_last()
    )
    checked = joined.withColumn("_dup_rn", F.row_number().over(duplicate_window))
    reason = F.concat_ws("; ",
        F.when(F.col("venta_id").isNull(), "venta_id nulo"),
        F.when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "cantidad no positiva o inválida"),
        F.when(F.col("monto_total").isNull() | (F.col("monto_total") <= 0), "monto no positivo o inválido"),
        F.when(F.col("fecha_venta").isNull(), "fecha_venta inválida"),
        F.when(F.col("producto_id").isNull() | F.col("_producto_ok").isNull(), "producto inexistente en catálogo"),
        F.when((F.col("canal") == "tienda_fisica") & F.col("vendedor_id").isNull(), "venta física sin vendedor"),
        F.when(F.col("canal").isNull(), "canal desconocido"),
        F.when(F.col("metodo_pago").isNull(), "método de pago desconocido"),
        F.when(F.col("_dup_rn") > 1, "venta_id duplicado; se conserva la versión más reciente")
    )
    bad = checked.withColumn("_reason", reason).filter(F.length("_reason") > 0)
    return _quarantine(bad, "ventas", "venta_id", F.lit("reglas_ventas"), F.col("_reason"))


@dp.materialized_view(name=f"{SILVER}.ventas",
                      comment="Ventas limpias, normalizadas y deduplicadas")
@dp.expect_or_drop("monto_positivo", "monto_total > 0")
def silver_ventas():
    v = _ventas_normalizadas()
    catalog = spark.read.table(f"{SILVER}.catalogo").select("producto_id").distinct()
    valid = (v.join(catalog, "producto_id", "inner")
        .filter(
            F.col("venta_id").isNotNull()
            & F.col("cantidad").isNotNull() & (F.col("cantidad") > 0)
            & F.col("monto_total").isNotNull() & (F.col("monto_total") > 0)
            & F.col("fecha_venta").isNotNull()
            & F.col("metodo_pago").isin("PLIN", "YAPE", "TRANSFERENCIA", "EFECTIVO", "TARJETA")
            & F.col("canal").isin("tienda_fisica", "ecommerce")
            & ~((F.col("canal") == "tienda_fisica") & F.col("vendedor_id").isNull())
        )
    )
    w = Window.partitionBy("venta_id").orderBy(F.col("ingestion_timestamp").desc_nulls_last())
    return valid.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# ==================================================================
# SILVER - EMPLEADOS SCD TYPE 2
# La vista materializada reconstruye intervalos al llegar eventos tardíos.
# ==================================================================

def _empleados_normalizados():
    raw = spark.read.table(f"{BRONZE}.empleados_raw")
    event = F.upper(F.regexp_replace(F.trim(F.col("tipo_evento")), r"[\s-]+", "_"))
    event = F.when(event == "CESE", "BAJA").otherwise(event)
    return (raw
        .withColumn("id_empleado", _empty_to_null("id_empleado"))
        .withColumn("nombre", _empty_to_null("nombre"))
        .withColumn("dni", _empty_to_null("dni"))
        .withColumn("email", _empty_to_null("email"))
        .withColumn("sucursal_id", _empty_to_null("sucursal_id"))
        .withColumn("cargo", _empty_to_null("cargo"))
        .withColumn("tipo_evento", event)
        .withColumn("salario", _decimal12("salario"))
        .withColumn("fecha_evento", _date3("fecha_evento"))
        .withColumn("fecha_registro", F.coalesce(
            F.expr("try_to_timestamp(fecha_registro, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("""try_to_timestamp(fecha_registro, "yyyy-MM-dd'T'HH:mm:ss")"""),
            F.to_timestamp("fecha_registro")
        ))
    )


@dp.materialized_view(name=f"{SILVER}.cuarentena_empleados",
                      comment="Eventos laborales no ubicables, tipo desconocido o salario fuera de rango")
def cuarentena_empleados():
    e = _empleados_normalizados()
    reason = F.concat_ws("; ",
        F.when(F.col("id_empleado").isNull(), "id_empleado nulo"),
        F.when(F.col("fecha_evento").isNull(), "fecha_evento nula o inválida"),
        F.when(~F.col("tipo_evento").isin("ALTA", "TRANSFERENCIA", "CAMBIO_SALARIO", "BAJA"), "tipo_evento desconocido"),
        F.when(F.col("salario").isNotNull() & ((F.col("salario") <= 0) | (F.col("salario") > 10000)), "salario fuera del rango permitido"),
        F.when(F.col("fecha_registro").isNull(), "fecha_registro nula o inválida")
    )
    bad = e.withColumn("_reason", reason).filter(F.length("_reason") > 0)
    return _quarantine(bad, "empleados", "id_empleado", F.lit("evento_y_salario_validos"), F.col("_reason"))


@dp.materialized_view(
                        name=f"{SILVER}.empleados",
                        comment="Historial laboral SCD Type 2; eventos tardíos recalculan intervalos",
                        override_schema="""
                            dni STRING MASK electrocasa.silver.mask_dni,
                            salario DECIMAL(12,2) MASK electrocasa.silver.mask_salario
                        """)
def silver_empleados():
    e = _empleados_normalizados().filter(
        F.col("id_empleado").isNotNull()
        & F.col("fecha_evento").isNotNull()
        & (F.col("salario").isNull() | ((F.col("salario") > 0) & (F.col("salario") <= 10000)))
        & F.col("tipo_evento").isin("ALTA", "TRANSFERENCIA", "CAMBIO_SALARIO", "BAJA")
        & F.col("fecha_registro").isNotNull()
    )
    event_cols = ["id_empleado", "nombre", "dni", "email", "salario",
                  "sucursal_id", "cargo", "tipo_evento", "fecha_evento", "fecha_registro"]
    e = e.dropDuplicates(event_cols)
    w = Window.partitionBy("id_empleado").orderBy(
        F.col("fecha_evento").asc(), F.col("fecha_registro").asc()
    )
    sequenced = (e
        .withColumn("_seq", F.row_number().over(w))
        .withColumn("valid_from_ts", F.expr("timestampadd(SECOND, cast(_seq - 1 as int), cast(fecha_evento as timestamp))"))
        .withColumn("_next_valid_from_ts", F.lead("valid_from_ts").over(w))
        .withColumn("valid_to_ts", F.expr("timestampadd(MICROSECOND, -1, _next_valid_from_ts)"))
        .withColumn("is_current", F.col("_next_valid_from_ts").isNull())
        .withColumn("valid_from", F.col("fecha_evento"))
        # Para consulta diaria, la última versión de un mismo día representa ese día.
        .withColumn("_next_event_date", F.lead("fecha_evento").over(w))
        .withColumn("valid_to", F.date_sub(F.col("_next_event_date"), 1))
    )
    return sequenced.select(
        "id_empleado", "nombre", "dni", "email", "salario",
        "sucursal_id", "cargo", "tipo_evento", "fecha_evento",
        "fecha_registro", "valid_from", "valid_to",
        "valid_from_ts", "valid_to_ts", "is_current",
        "_source_file", "_source_file_name", "_source_system",
        "ingestion_timestamp", "ingestion_batch_id"
    )


# ==================================================================
# SILVER - RESEÑAS
# ==================================================================

RESPUESTA_SCHEMA = ArrayType(StructType([
    StructField("autor", StringType(), True),
    StructField("texto", StringType(), True),
    StructField("fecha", StringType(), True),
]))

RESENA_SCHEMA = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("calificacion", IntegerType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("respuestas", RESPUESTA_SCHEMA, True),
    StructField("fecha_resena", StringType(), True),
])


def _resenas_parseadas():
    raw = spark.read.table(f"{BRONZE}.resenas_raw")
    parsed = (raw
        .withColumn("_json", F.from_json("raw_json", RESENA_SCHEMA))
        .select(
            "_source_file", "_source_file_name", "_source_system",
            "ingestion_timestamp", "ingestion_batch_id", "raw_json",
            F.col("_json.resena_id").alias("resena_id"),
            F.col("_json.producto_id").alias("producto_id"),
            F.col("_json.cliente_id").alias("cliente_id"),
            F.col("_json.calificacion").cast("int").alias("calificacion"),
            F.col("_json.comentario").alias("comentario"),
            F.col("_json.tags").alias("tags"),
            F.col("_json.respuestas").alias("respuestas"),
            F.col("_json.fecha_resena").alias("fecha_resena")
        )
        .withColumn("fecha_resena", _date3("fecha_resena"))
        .withColumn("comentario", F.when(F.trim("comentario") == "", None).otherwise(F.trim("comentario")))
        .withColumn("tags", F.coalesce(F.col("tags"), F.from_json(F.lit("[]"), "array<string>")))
        .withColumn("respuestas", F.coalesce(F.col("respuestas"), F.from_json(F.lit("[]"), RESPUESTA_SCHEMA)))
        .withColumn("respuestas", F.expr(
            "transform(respuestas, x -> named_struct("
            "'autor', x.autor, 'texto', x.texto, "
            "'fecha', coalesce(try_to_date(x.fecha, 'yyyy-MM-dd'), try_to_date(x.fecha, 'dd/MM/yyyy'))))"
        ))
        .withColumn("metadata", F.expr("try_parse_json(get_json_object(raw_json, '$.metadata'))"))
    )
    return parsed.drop("raw_json")


@dp.materialized_view(name=f"{SILVER}.cuarentena_resenas",
                      comment="Reseñas inválidas o con referencia de producto inexistente")
def cuarentena_resenas():
    r = _resenas_parseadas()
    products = spark.read.table(f"{SILVER}.catalogo").select("producto_id").distinct()
    joined = r.join(products.withColumn("_producto_ok", F.lit(1)), "producto_id", "left")
    joined = joined.withColumn("_dup_rn", F.row_number().over(
        Window.partitionBy("resena_id").orderBy(F.col("ingestion_timestamp").desc_nulls_last(),
                                                F.col("_source_file_name").asc_nulls_last())))
    missing_author = F.expr("exists(respuestas, x -> x.autor is null or trim(x.autor) = '')")
    reason = F.concat_ws("; ",
        F.when(F.col("resena_id").isNull(), "resena_id nulo"),
        F.when(F.col("calificacion").isNull() | ~F.col("calificacion").between(1, 5), "calificación fuera de 1 a 5"),
        F.when(F.col("producto_id").isNull() | F.col("_producto_ok").isNull(), "producto inexistente en catálogo"),
        F.when(missing_author, "respuesta sin autor; se conserva sin inventar autor"),
        F.when(F.col("_dup_rn") > 1, "resena_id duplicado; se conserva la versión más reciente")
    )
    bad = joined.withColumn("_reason", reason).filter(F.length("_reason") > 0)
    return _quarantine(bad, "resenas", "resena_id", F.lit("reglas_resenas"), F.col("_reason"))


@dp.materialized_view(name=f"{SILVER}.resenas",
                      comment="Reseñas limpias con campos anidados y metadata VARIANT")
def silver_resenas():
    r = _resenas_parseadas()
    products = spark.read.table(f"{SILVER}.catalogo").select("producto_id").distinct()
    valid = (r.join(products, "producto_id", "inner")
        .filter(
            F.col("resena_id").isNotNull()
            & F.col("calificacion").between(1, 5)
            & F.col("fecha_resena").isNotNull()
        )
    )
    w = Window.partitionBy("resena_id").orderBy(F.col("ingestion_timestamp").desc())
    return (valid.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1).drop("_rn"))


# ==================================================================
# SILVER - DEVOLUCIONES
# ==================================================================

def _devoluciones_normalizadas():
    raw = spark.read.table(f"{BRONZE}.devoluciones_raw")
    return (raw
        .withColumn("devolucion_id", _empty_to_null("devolucion_id"))
        .withColumn("venta_id", _empty_to_null("venta_id"))
        .withColumn("sucursal_id", _empty_to_null("sucursal_id"))
        .withColumn("producto_id", _empty_to_null("producto_id"))
        .withColumn("motivo", _empty_to_null("motivo"))
        .withColumn("monto_reembolso", _decimal12("monto_reembolso"))
        .withColumn("fecha_devolucion", _date3("fecha_devolucion"))
    )


@dp.materialized_view(name=f"{SILVER}.cuarentena_devoluciones",
                      comment="Devoluciones sin venta válida o con reembolso inválido")
def cuarentena_devoluciones():
    d = _devoluciones_normalizadas()
    sales = spark.read.table(f"{SILVER}.ventas").select("venta_id", F.col("monto_total").alias("_venta_monto"))
    joined = d.join(sales, "venta_id", "left").withColumn(
        "_dup_rn", F.row_number().over(Window.partitionBy("devolucion_id").orderBy(
            F.col("ingestion_timestamp").desc_nulls_last(), F.col("_source_file_name").asc_nulls_last())))
    reason = F.concat_ws("; ",
        F.when(F.col("devolucion_id").isNull(), "devolucion_id nulo"),
        F.when(F.col("venta_id").isNull() | F.col("_venta_monto").isNull(), "venta vacía o inexistente"),
        F.when(F.col("monto_reembolso").isNull() | (F.col("monto_reembolso") < 0)
               | (F.col("monto_reembolso") > F.col("_venta_monto")), "reembolso negativo, nulo o mayor a la venta"),
        F.when(F.col("fecha_devolucion").isNull(), "fecha de devolución inválida"),
        F.when(F.col("_dup_rn") > 1, "devolucion_id duplicado; se conserva una versión")
    )
    bad = joined.withColumn("_reason", reason).filter(F.length("_reason") > 0)
    return _quarantine(bad, "devoluciones", "devolucion_id", F.lit("reglas_devoluciones"), F.col("_reason"))


@dp.materialized_view(name=f"{SILVER}.devoluciones",
                      comment="Devoluciones válidas relacionadas con su venta original")
def silver_devoluciones():
    d = _devoluciones_normalizadas()
    sales = spark.read.table(f"{SILVER}.ventas").select("venta_id", F.col("monto_total").alias("_venta_monto"))
    valid = (d.join(sales, "venta_id", "inner")
        .filter(
            F.col("devolucion_id").isNotNull()
            & F.col("monto_reembolso").isNotNull()
            & (F.col("monto_reembolso") >= 0)
            & (F.col("monto_reembolso") <= F.col("_venta_monto"))
            & F.col("fecha_devolucion").isNotNull()
        )
    )
    w = Window.partitionBy("devolucion_id").orderBy(F.col("ingestion_timestamp").desc())
    return (valid.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1).drop("_rn", "_venta_monto"))


# ==================================================================
# SILVER - TRACKING (Bronze cargada por Lakeflow Connect)
# ==================================================================

def _tracking_normalizado():
    raw = _ensure_audit(spark.read.table(f"{BRONZE}.tracking_envios"), "tracking_envios")
    state_raw = F.lower(F.regexp_replace(F.trim(F.col("estado_entrega")), r"[\s-]+", "_"))
    state = (F.when(state_raw.isin("en_camino", "en_transito", "en_transito"), "en_transito")
               .when(state_raw == "pendiente", "pendiente")
               .when(state_raw == "entregado", "entregado")
               .when(state_raw == "devuelto", "devuelto"))
    return (raw
        .withColumn("evento_id", F.col("evento_id").cast("int"))
        .withColumn("tracking_id", _empty_to_null("tracking_id"))
        .withColumn("venta_id", _empty_to_null("venta_id"))
        .withColumn("courier", F.initcap(F.trim(F.col("courier"))))
        .withColumn("estado_entrega", state)
        .withColumn("sucursal_origen", _empty_to_null("sucursal_origen"))
        .withColumn("fecha_actualizacion", F.to_date("fecha_actualizacion"))
        .withColumn("fecha_modificacion", F.to_timestamp("fecha_modificacion"))
    )


@dp.materialized_view(name=f"{SILVER}.cuarentena_tracking_envios",
                      comment="Eventos de tracking con estado desconocido, venta huérfana o datos clave inválidos")
def cuarentena_tracking_envios():
    t = _tracking_normalizado()
    sales = spark.read.table(f"{SILVER}.ventas").select("venta_id").distinct()
    joined = t.join(sales.withColumn("_venta_ok", F.lit(1)), "venta_id", "left")
    reason = F.concat_ws("; ",
        F.when(F.col("evento_id").isNull(), "evento_id nulo"),
        F.when(F.col("tracking_id").isNull(), "tracking_id nulo"),
        F.when(F.col("estado_entrega").isNull(), "estado de entrega desconocido"),
        F.when(F.col("fecha_modificacion").isNull(), "fecha_modificacion nula"),
        F.when(F.col("venta_id").isNotNull() & F.col("_venta_ok").isNull(), "venta asociada inexistente")
    )
    bad = joined.withColumn("_reason", reason).filter(F.length("_reason") > 0)
    return _quarantine(bad, "tracking_envios", "evento_id", F.lit("reglas_tracking"), F.col("_reason"))


@dp.materialized_view(name=f"{SILVER}.tracking_envios",
                      comment="Historial de tracking limpio y sin mensajes reprocesados")
def silver_tracking_envios():
    t = _tracking_normalizado()
    sales = spark.read.table(f"{SILVER}.ventas").select("venta_id").distinct()
    cleaned = (t.join(sales.withColumn("_venta_ok", F.lit(1)), "venta_id", "left")
        .filter(
            F.col("evento_id").isNotNull()
            & F.col("tracking_id").isNotNull()
            & F.col("fecha_modificacion").isNotNull()
            & F.col("estado_entrega").isin("pendiente", "en_transito", "entregado", "devuelto")
            & (F.col("venta_id").isNull() | F.col("_venta_ok").isNotNull())
        )
        .withColumn("_business_time", F.coalesce(
            F.col("fecha_actualizacion").cast("string"),
            F.col("fecha_modificacion").cast("string")
        ))
    )
    # evento_id es la PK del conector; el dedup de negocio detecta reenvíos con distinto evento_id.
    w_event = Window.partitionBy("evento_id").orderBy(F.col("fecha_modificacion").desc())
    dedup_event = cleaned.withColumn("_rn_event", F.row_number().over(w_event)).filter(F.col("_rn_event") == 1).drop("_rn_event")
    w_business = Window.partitionBy("tracking_id", "estado_entrega", "_business_time").orderBy(
        F.col("fecha_modificacion").asc(), F.col("evento_id").asc()
    )
    return (dedup_event.withColumn("_rn_business", F.row_number().over(w_business))
            .filter(F.col("_rn_business") == 1)
            .drop("_rn_business", "_business_time", "_venta_ok"))


# ==================================================================
# GOLD - SEIS PREGUNTAS DEL ENUNCIADO
# ==================================================================

@dp.materialized_view(name=f"{GOLD}.ventas_netas_sucursal_mes",
                      comment="Ventas netas, devoluciones, cantidad y ticket promedio por sucursal y mes")
def gold_ventas_netas_sucursal_mes():
    v = spark.read.table(f"{SILVER}.ventas").withColumn("mes", F.date_trunc("month", "fecha_venta").cast("date"))
    d = spark.read.table(f"{SILVER}.devoluciones").withColumn("mes", F.date_trunc("month", "fecha_devolucion").cast("date"))
    vs = v.groupBy("sucursal_id", "mes").agg(
        F.sum("monto_total").alias("ventas_brutas"),
        F.countDistinct("venta_id").alias("cantidad_ventas")
    )
    ds = d.groupBy("sucursal_id", "mes").agg(F.sum("monto_reembolso").alias("devoluciones"))
    return (vs.join(ds, ["sucursal_id", "mes"], "full")
        .fillna({"ventas_brutas": 0, "cantidad_ventas": 0, "devoluciones": 0})
        .withColumn("ventas_netas", F.col("ventas_brutas") - F.col("devoluciones"))
        .withColumn("ticket_promedio", F.when(F.col("cantidad_ventas") > 0, F.col("ventas_brutas") / F.col("cantidad_ventas")))
    )


@dp.materialized_view(name=f"{GOLD}.top_productos_unidades",
                      comment="Top 10 de productos por unidades vendidas")
def gold_top_productos_unidades():
    v = spark.read.table(f"{SILVER}.ventas")
    c = spark.read.table(f"{SILVER}.catalogo")
    c = (c.withColumn("_cat_rn", F.row_number().over(
            Window.partitionBy("producto_id").orderBy(F.col("fecha_snapshot").desc())))
           .filter(F.col("_cat_rn") == 1).drop("_cat_rn"))
    totals = v.groupBy("producto_id").agg(F.sum("cantidad").alias("unidades_vendidas"))
    w = Window.orderBy(F.col("unidades_vendidas").desc(), F.col("producto_id"))
    return (totals.join(c, "producto_id", "left")
        .withColumn("ranking", F.row_number().over(w))
        .filter(F.col("ranking") <= 10))


@dp.materialized_view(name=f"{GOLD}.top_productos_tasa_devolucion",
                      comment="Top 10 de productos por tasa de devoluciones")
def gold_top_productos_tasa_devolucion():
    v = spark.read.table(f"{SILVER}.ventas").groupBy("producto_id").agg(F.countDistinct("venta_id").alias("ventas"))
    d = spark.read.table(f"{SILVER}.devoluciones").groupBy("producto_id").agg(F.countDistinct("devolucion_id").alias("devoluciones"))
    rates = (v.join(d, "producto_id", "left").fillna({"devoluciones": 0})
        .withColumn("tasa_devolucion", F.col("devoluciones") / F.col("ventas"))
        .filter(F.col("ventas") > 0))
    w = Window.orderBy(F.col("tasa_devolucion").desc(), F.col("producto_id"))
    return rates.withColumn("ranking", F.row_number().over(w)).filter(F.col("ranking") <= 10)


@dp.materialized_view(
    name=f"{GOLD}.empleados_activos_sucursal_mes",
    comment="Empleados activos por sucursal al cierre de cada mes marzo-octubre 2026"
)
def gold_empleados_activos_sucursal_mes():

    e = spark.read.table(f"{SILVER}.empleados")

    months = spark.sql("""
        SELECT explode(
            sequence(
                to_date('2026-03-31'),
                to_date('2026-10-31'),
                interval 1 month
            )
        ) AS fecha_corte
    """).withColumn("fecha_corte", F.last_day("fecha_corte"))

    active = (
        months.alias("m")
        .join(
            e.alias("e"),
            (F.col("e.valid_from") <= F.col("m.fecha_corte"))
            & (
                F.col("e.valid_to").isNull()
                | (F.col("e.valid_to") >= F.col("m.fecha_corte"))
            ),
            "inner"
        )
        .filter(F.col("e.tipo_evento") != "BAJA")
    )

    return (
        active
        .groupBy(
            F.col("m.fecha_corte").alias("fecha_corte"),
            F.col("e.sucursal_id").alias("sucursal_id")
        )
        .agg(
            F.countDistinct("e.id_empleado").alias("empleados_activos")
        )
    )


@dp.materialized_view(name=f"{GOLD}.ventas_por_vendedor_sucursal_mes",
                      comment="Ventas físicas por vendedor y sucursal histórica en la fecha de venta")
def gold_ventas_por_vendedor_sucursal_mes():
    v = spark.read.table(f"{SILVER}.ventas").filter(F.col("canal") == "tienda_fisica").alias("v")
    e = spark.read.table(f"{SILVER}.empleados").alias("e")
    joined = v.join(
        e,
        (F.col("v.vendedor_id") == F.col("e.id_empleado"))
        & (F.col("v.fecha_venta") >= F.col("e.valid_from"))
        & (F.col("e.valid_to").isNull() | (F.col("v.fecha_venta") <= F.col("e.valid_to")))
        & (F.col("e.tipo_evento") != "BAJA"),
        "left"
    )
    return joined.groupBy(
        F.col("v.vendedor_id").alias("vendedor_id"),
        F.coalesce(F.col("e.sucursal_id"), F.lit("SIN_ASIGNAR")).alias("sucursal_id"),
        F.date_trunc("month", F.col("v.fecha_venta")).cast("date").alias("mes")
    ).agg(
        F.sum(F.col("v.monto_total")).alias("monto_ventas"),
        F.countDistinct(F.col("v.venta_id")).alias("cantidad_ventas")
    )


@dp.materialized_view(name=f"{GOLD}.resenas_negativas_categoria_mes",
                      comment="Porcentaje de reseñas negativas por categoría y mes")
def gold_resenas_negativas_categoria_mes():
    r = spark.read.table(f"{SILVER}.resenas").alias("r")
    c = spark.read.table(f"{SILVER}.catalogo").alias("c")
    latest = (c.withColumn("_rn", F.row_number().over(
        Window.partitionBy("producto_id").orderBy(F.col("fecha_snapshot").desc())
    )).filter(F.col("_rn") == 1).select("producto_id", "categoria"))
    joined = r.join(latest, "producto_id", "inner")
    return joined.groupBy(
        "categoria",
        F.date_trunc("month", "fecha_resena").cast("date").alias("mes")
    ).agg(
        F.count("*").alias("total_resenas"),
        F.sum(F.when(F.col("calificacion").isin(1, 2), 1).otherwise(0)).alias("resenas_negativas")
    ).withColumn(
        "porcentaje_negativas",
        F.when(F.col("total_resenas") > 0, F.col("resenas_negativas") / F.col("total_resenas") * 100)
    )


@dp.materialized_view(name=f"{GOLD}.despachos_por_courier",
                      comment="Tiempo desde primer pendiente hasta primer entregado y porcentaje devuelto por courier")
def gold_despachos_por_courier():
    t = spark.read.table(f"{SILVER}.tracking_envios")
    # Una fila por envío; se atribuye al courier del último evento observado.
    courier_window = Window.partitionBy("tracking_id").orderBy(
        F.col("fecha_modificacion").desc(), F.col("evento_id").desc()
    )
    latest_courier = (t.withColumn("_rn", F.row_number().over(courier_window))
        .filter(F.col("_rn") == 1)
        .select("tracking_id", "courier"))
    milestones = t.groupBy("tracking_id").agg(
        F.min(F.when(F.col("estado_entrega") == "pendiente", F.col("fecha_modificacion"))).alias("primer_pendiente"),
        F.min(F.when(F.col("estado_entrega") == "entregado", F.col("fecha_modificacion"))).alias("primer_entregado"),
        F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("tiene_devuelto"),
        F.max(F.when(F.col("estado_entrega").isin("entregado", "devuelto"), 1).otherwise(0)).alias("tiene_estado_final")
    )
    per_shipment = (milestones.join(latest_courier, "tracking_id", "left")
        .withColumn("horas_hasta_entrega",
            (F.col("primer_entregado").cast("long") - F.col("primer_pendiente").cast("long")) / 3600.0)
    )
    return per_shipment.groupBy("courier").agg(
        F.avg("horas_hasta_entrega").alias("promedio_horas_entrega"),
        F.sum(F.when(F.col("tiene_estado_final") == 1, 1).otherwise(0)).alias("envios_estado_final"),
        F.sum(F.when((F.col("tiene_estado_final") == 1) & (F.col("tiene_devuelto") == 1), 1).otherwise(0)).alias("envios_devueltos")
    ).withColumn(
        "porcentaje_envios_devueltos",
        F.when(F.col("envios_estado_final") > 0,
               F.col("envios_devueltos") / F.col("envios_estado_final") * 100)
    )


@dp.materialized_view(
    name=f"{GOLD}.empleados",
    comment="Vista de empleados para demostrar masking y acceso diferenciado",
    override_schema="""
        dni STRING MASK electrocasa.silver.mask_dni,
        salario DECIMAL(12,2) MASK electrocasa.silver.mask_salario
    """
)
def gold_empleados():
    empleados = spark.read.table(f"{SILVER}.empleados")

    return empleados.filter(
        F.col("is_current") == True
    ).select(
        "id_empleado",
        "nombre",
        "dni",
        "salario",
        "sucursal_id",
        "cargo",
        "tipo_evento",
        "valid_from",
        "valid_to",
        "is_current"
    )

