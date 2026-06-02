from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    current_date,
    date_format,
    row_number,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType
)
from pyspark.sql.window import Window


# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

REGEX_TELEFONE = r"^\(\d{2}\)\d{5}-\d{4}$"


spark = (
    SparkSession.builder
    .appName("teste_eng_dados_3")
    .getOrCreate()
)


# ============================================================================
# EXPLORAÇÃO INICIAL DOS DADOS
# ============================================================================

RAW_PATH = "s3://bucket-silver/tb_cliente"

# Utilizado apenas para entendimento do dataset e definição do schema

df_explorer = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW_PATH)
)

df_explorer.printSchema()
df_explorer.show(2, truncate=False)


# ============================================================================
# DEFINIÇÃO DO SCHEMA
# ============================================================================

schema_clientes = StructType([
    StructField("cod_cliente", IntegerType(), True),
    StructField("nm_cliente", StringType(), True),
    StructField("nm_pais_cliente", StringType(), True),
    StructField("nm_cidade_cliente", StringType(), True),
    StructField("nm_rua_cliente", StringType(), True),
    StructField("num_casa_cliente", IntegerType(), True),
    StructField("telefone_cliente", StringType(), True),
    StructField("dt_nascimento_cliente", DateType(), True),
    StructField("dt_atualizacao", DateType(), True),
    StructField("tp_pessoa", StringType(), True),
    StructField("vl_renda", DoubleType(), True),
])


# ============================================================================
# LEITURA OFICIAL DA CAMADA RAW
# ============================================================================

df_raw = (
    spark.read
    .option("header", True)
    .schema(schema_clientes)
    .csv(RAW_PATH)
)

df_raw.show(5, truncate=True)


# ============================================================================
# CAMADA BRONZE
# ============================================================================

# Regras aplicadas:
# - Padronização dos nomes em caixa alta
# - Renomeação da coluna de telefone
# - Inclusão da partição anomesdia

df_bronze = (
    df_raw
    .withColumn("nm_cliente", upper(col("nm_cliente")))
    .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
    .withColumn("anomesdia", date_format(current_date(), "yyyyMMdd"))
)

# Validação dos resultados Bronze

df_bronze.orderBy("cod_cliente").show(5, truncate=False)


# Escrita Bronze

BRONZE_PATH = "s3://bucket-bronze/tabela_cliente_landing"

# (
#     df_bronze.write
#     .mode("overwrite")
#     .partitionBy("anomesdia")
#     .parquet(BRONZE_PATH)
# )


# ============================================================================
# CAMADA SILVER
# ============================================================================

# Regras aplicadas:
# - Deduplicação por cliente
# - Manutenção da versão mais recente do cadastro
# - Validação do padrão de telefone

window_cliente = (
    Window
    .partitionBy("cod_cliente")
    .orderBy(col("dt_atualizacao").desc())
)

df_silver = (
    df_bronze
    .withColumn("rn", row_number().over(window_cliente))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn(
        "num_telefone_cliente",
        when(
            col("num_telefone_cliente").rlike(REGEX_TELEFONE),
            col("num_telefone_cliente")
        ).otherwise(None)
    )
)

# Validação dos resultados Silver

df_silver.orderBy("cod_cliente").show(30, truncate=False)


# Escrita Silver

SILVER_PATH = "s3://bucket-silver/tb_cliente"

# (
#     df_silver.write
#     .mode("overwrite")
#     .partitionBy("anomesdia")
#     .parquet(SILVER_PATH)
# )


spark.stop()