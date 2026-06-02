from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    current_date,
    date_format
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

from class_DQR import DQRSilver

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================


SILVER_PATH = "s3://bucket-silver/tb_cliente"

spark = (
    SparkSession.builder
    .appName("teste_eng_dados_3")
    .getOrCreate()
)

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
# LEITURA DA CAMADA SILVER
# ============================================================================

df_silver = (
    spark.read
    .option("header", True)
    .schema(schema_clientes)
    .csv(SILVER_PATH)
    .withColumn("nm_cliente", upper(col("nm_cliente")))
    .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
    .withColumn("anomesdia", date_format(current_date(), "yyyyMMdd"))
)

df_silver.orderBy("cod_cliente").show(30, truncate=False)

#=================================================
# RELATÓRIO DE DATA QUALITY
#=================================================

dqr = DQRSilver(df_silver)

dqr.executar_validacoes()

df_resultado = dqr.gerar_relatorio()

df_resultado.show()
