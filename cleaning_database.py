import pandas as pd
from logs import infos_auxiliares, relatorio_qualidade
import logging

##########################################
# ⚙️ CONFIG LOG
##########################################

logging.basicConfig(
    filename="caminho/logs.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)


def log_section(txt):
    logging.info(txt)
    logging.info("\n" + "="*50)
    logging.info("="*50)

##########################################
# 🔎 ANÁLISE
##########################################


def log(msg):
    print(msg)
    logging.info(msg)


def info_basic(df):
    log(f"\n===== INFO BÁSICA =====")
    log(f"HEAD:\n{df.head(10)}")
    log(f"Shape: {df.shape}")
    log(f"Describe:\n{df.describe()}")
    log(f"Tipos:\n{df.dtypes}")
    log(f"Colunas: {list(df.columns)}")


def padronizar_colunas(df):
    logging.info("Padronizando nomes das colunas...")

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    return df


def padronizar_textos(df):

    logging.info("Padronizando textos...")

    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

    logging.info("Padronizando categorias...")

    logging.info("Convertendo dados...")

    return df


def tratar_nulos(df):
    logging.info("Tratando valores nulos...")

    return df


def tratar_duplicados(df):
    log_section("Tratando duplicadas...")
    duplicados = df.duplicated().sum()
    logging.info(f"\nDuplicados encontrados: {duplicados}")

    logging.info("Removendo duplicados...")

    antes = len(df)

    df = df.drop_duplicates()

    depois = len(df)

    logging.info(f"Removidos {antes - depois} registros duplicados")
    return df


def validar_dados(df):
    logging.info("Validando dados...")

    antes = len(df)

    depois = len(df)

    logging.info(f"Total removido: {antes - depois}")

    return df


def pipeline(df):

    log_section("🚀 INÍCIO DA PIPELINE")

    info_basic(df)
    infos_auxiliares(df)

    df = padronizar_colunas(df)
    df = padronizar_textos(df)
    df = tratar_nulos(df)
    df = tratar_duplicados(df)
    df = validar_dados(df)

    info_basic(df)

    log_section("✅ PIPELINE FINALIZADA")

    return df


if __name__ == "__main__":

    df = pd.read_csv("caminho/do/dado.csv")

    df_limpo = pipeline(df)
    relatorio_qualidade(df_limpo, df)
    df_limpo.to_csv("caminho/do/dado.csv", index=False)
