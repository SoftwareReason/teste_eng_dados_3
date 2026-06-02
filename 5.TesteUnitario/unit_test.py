import pytest
from pyspark.sql import SparkSession
from test_class_DQR import DQRSilver
from datetime import date, timedelta

@pytest.fixture(scope="session")

def spark():
    return (
        SparkSession.builder
        .appName("unit_tests_dqr_silver")
        .master("local[*]")
        .getOrCreate()
    )

def test_adicionar_resultado_deve_calcular_percentual_e_status_corretamente(spark):

    dados = [
        (1,),
        (2,),
        (3,),
        (4,),
        (5,),
        (6,),
        (7,),
        (8,),
        (9,),
        (10,)
    ]

    colunas = ["cod_cliente"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.adicionar_resultado(
        "Validade da Renda",
        2
    )

    resultado = dqr.resultados[0]

    assert len(dqr.resultados) == 1
    assert resultado["regra"] == "Validade da Renda"
    assert resultado["qtd_invalidos"] == 2
    assert resultado["percentual_invalidos"] == 20.0
    assert resultado["status"] == "FAIL"


def test_validar_unicidade_cliente_deve_identificar_cliente_duplicado(spark):

    dados = [
        (1,),
        (1,),
        (2,)
    ]

    colunas = ["cod_cliente"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_unicidade_cliente()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Unicidade do Cliente"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"


def test_validar_padrao_telefone_deve_identificar_telefone_nulo(spark):

    dados = [
        ("(11)99999-9999",),
        (None,),
        ("(21)98888-8888",)
    ]

    colunas = ["num_telefone_cliente"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)
 
    dqr.validar_padrao_telefone()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Qualidade / Padrão de Telefone"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"


def test_validar_data_atualizacao_deve_identificar_data_futura(spark):

    hoje = date.today()
    data_futura = hoje + timedelta(days=1)

    dados = [
        (hoje,),
        (data_futura,),
        (hoje,)
    ]

    colunas = ["dt_atualizacao"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_data_atualizacao()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Validade Temporal da Atualização"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"



def test_validar_tp_pessoa_deve_identificar_valor_fora_do_dominio(spark):

    dados = [
        ("PF",),
        ("PJ",),
        ("ABC",)
    ]

    colunas = ["tp_pessoa"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_tp_pessoa()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Domínio Válido de Tipo de Pessoa"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"



def test_validar_renda_deve_identificar_renda_negativa(spark):

    dados = [
        (1000.0,),
        (0.0,),
        (-500.0,)
    ]

    colunas = ["vl_renda"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_renda()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Validade da Renda"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"



def test_validar_data_nascimento_futura_deve_identificar_nascimento_futuro(spark):

    hoje = date.today()
    data_futura = hoje + timedelta(days=1)

    dados = [
        (date(1990, 5, 10),),
        (data_futura,),
        (date(2000, 1, 1),)
    ]

    colunas = ["dt_nascimento_cliente"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_data_nascimento_futura()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Data de Nascimento Futura"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"



def test_validar_nascimento_maior_atualizacao_deve_identificar_inconsistencia_temporal(spark):

    dados = [
        (date(1990, 5, 10), date(2024, 1, 1)),
        (date(2030, 1, 1), date(2024, 1, 1)),
        (date(2000, 8, 20), date(2024, 1, 1))
    ]

    colunas = [
        "dt_nascimento_cliente",
        "dt_atualizacao"
    ]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_nascimento_maior_atualizacao()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Nascimento Posterior à Atualização"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"


def test_validar_idade_plausivel_deve_identificar_idade_acima_do_limite(spark):

    dados = [
        (date(1990, 1, 1),),
        (date(1800, 1, 1),),
        (date(2000, 1, 1),)
    ]

    colunas = ["dt_nascimento_cliente"]

    df = spark.createDataFrame(dados, colunas)

    dqr = DQRSilver(df)

    dqr.validar_idade_plausivel()

    resultado = dqr.resultados[0]

    assert resultado["regra"] == "Idade Plausível"
    assert resultado["qtd_invalidos"] == 1
    assert resultado["percentual_invalidos"] == 33.33
    assert resultado["status"] == "FAIL"