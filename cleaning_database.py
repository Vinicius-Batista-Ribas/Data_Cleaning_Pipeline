import pandas as pd
import logging

##########################################
# ⚙️ CONFIG LOG
##########################################

logging.basicConfig(
    filename="logs_pipeline.txt",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def log_section():
    logging.info("\n" + "="*50)
    logging.info("="*50)

##########################################
# 🔎 ANÁLISE
##########################################


def info_basic(df):

    log_section(f"ANÁLISE")

    logging.info(f"📏 Shape: {df.shape}")
    logging.info("\n🔍 HEAD:\n%s", df.head(10))
    logging.info("\n📊 DESCRIBE:\n%s", df.describe(include='all'))

##########################################
# 🧹 LIMPEZA PADRÃO
##########################################


def tratar_nulos(df):
    log_section("🧹 TRATAMENTO DE NULOS")

    return df


def tratar_duplicados(df):
    log_section("🔁 TRATAMENTO DE DUPLICADOS")

    return df

##########################################
# 🧩 PADRONIZAÇÃO
##########################################


def padronizar_textos(df):
    log_section("🧩 PADRONIZAÇÃO DE TEXTOS")

    return df


def padronizar_categorias(df):
    log_section("🧩 PADRONIZAÇÃO DE CATEGORIAS")
    logging.info("⚙️ Nenhuma regra aplicada")
    return df

##########################################
# ✅ VALIDAÇÃO
##########################################


def validar_dados(df):
    log_section("✅ VALIDAÇÃO DE DADOS")

    return df

##########################################
# 🚀 PIPELINE
##########################################


def pipeline(df):

    log_section("🚀 INÍCIO DA PIPELINE")

    info_basic(df)

    # df = tratar_nulos(df)
    # df = tratar_duplicados(df)
    # df = padronizar_textos(df)
    # df = padronizar_categorias(df)
    # df = validar_dados(df)

    info_basic(df, "DEPOIS")

    log_section("✅ PIPELINE FINALIZADA")

    return df

##########################################
# ▶ EXECUÇÃO
##########################################


if __name__ == "__main__":

    df = pd.read_csv("caminho/do/arquivo.csv")

    df_limpo = pipeline(df)

    df_limpo.to_csv("Example_2/dados_limpos.csv", index=False)
