from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    floor, 
    months_between
)


spark = SparkSession.builder.getOrCreate()

#=================================================
# RELATÓRIO DE DATA QUALITY
#=================================================

class DQRSilver:
    def __init__(self, df_silver):
        self.df_silver = df_silver
        self.total_registros = df_silver.count()
        self.resultados = []

    def adicionar_resultado(self, regra, qtd_invalidos):
        percentual = (qtd_invalidos / self.total_registros) * 100

        self.resultados.append({
            "regra": regra,
            "qtd_invalidos": qtd_invalidos,
            "percentual_invalidos": float(f"{percentual:.2f}"),
            "status": "PASS" if qtd_invalidos == 0 else "FAIL"
        })


    def validar_unicidade_cliente(self):
        qtd_invalidos = (
            self.df_silver
            .groupBy("cod_cliente")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        self.adicionar_resultado(
            "Unicidade do Cliente",
            qtd_invalidos
        )

    def validar_padrao_telefone(self):
        qtd_invalidos = (
            self.df_silver
            .filter(col("num_telefone_cliente").isNull())
            .count()
        )

        self.adicionar_resultado(
            "Qualidade / Padrão de Telefone",
            qtd_invalidos
        )

    def validar_data_atualizacao(self):
        qtd_invalidos = (
            self.df_silver
            .filter(col("dt_atualizacao") > current_date())
            .count()
        )

        self.adicionar_resultado(
            "Validade Temporal da Atualização",
            qtd_invalidos
        )

    def validar_tp_pessoa(self):
        qtd_invalidos = (
            self.df_silver
            .filter(~col("tp_pessoa").isin("PF", "PJ"))
            .count()
        )

        self.adicionar_resultado(
            "Domínio Válido de Tipo de Pessoa",
            qtd_invalidos
        )

    def validar_renda(self):
        qtd_invalidos = (
            self.df_silver
            .filter(col("vl_renda") < 0)
            .count()
        )

        self.adicionar_resultado(
            "Validade da Renda",
            qtd_invalidos
        )

    def validar_data_nascimento_futura(self):
        qtd_invalidos = (
            self.df_silver
            .filter(col("dt_nascimento_cliente") > current_date())
            .count()
        )

        self.adicionar_resultado(
            "Data de Nascimento Futura",
            qtd_invalidos
        )

    def validar_nascimento_maior_atualizacao(self):
        qtd_invalidos = (
            self.df_silver
            .filter(col("dt_nascimento_cliente") > col("dt_atualizacao"))
            .count()
        )

        self.adicionar_resultado(
            "Nascimento Posterior à Atualização",
            qtd_invalidos
        )

    def validar_idade_plausivel(self):
        df_idade = (
            self.df_silver
            .withColumn(
                "idade",
                floor(
                    months_between(
                        current_date(),
                        col("dt_nascimento_cliente")
                    ) / 12
                ).cast("int")
            )
        )

        qtd_invalidos = (
            df_idade
            .filter(
                (col("idade") < 0) |
                (col("idade") > 120)
            )
            .count()
        )

        self.adicionar_resultado(
            "Idade Plausível",
            qtd_invalidos
        )

    def executar_validacoes(self):
        self.validar_unicidade_cliente()
        self.validar_padrao_telefone()
        self.validar_data_atualizacao()
        self.validar_tp_pessoa()
        self.validar_renda()
        self.validar_data_nascimento_futura()
        self.validar_nascimento_maior_atualizacao()
        self.validar_idade_plausivel()

    
    def gerar_relatorio(self):
        print(f"Total de registros: {self.total_registros}")
        data_quality_report = spark.createDataFrame(self.resultados)
        return data_quality_report



