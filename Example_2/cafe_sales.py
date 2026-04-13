import pandas as pd
from logs import infos_auxiliares, relatorio_qualidade
import logging

##########################################
# ⚙️ CONFIG LOG
##########################################

logging.basicConfig(
    filename="Example_2/logs/logs_pipeline.txt",
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

    return df


def tratar_valores_invalidos(df):
    logging.info("Tratando valores inválidos...")

    valores_invalidos = ["error", "unknown"]

    for col in df.columns:
        df[col] = df[col].astype(str).str.lower()

        df[col] = df[col].replace(valores_invalidos, pd.NA)

    return df


def converter_tipos(df):
    logging.info("Convertendo tipos...")

    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
    df['total_spent'] = pd.to_numeric(df['total_spent'], errors='coerce')

    df['transaction_date'] = pd.to_datetime(
        df['transaction_date'], errors='coerce')

    # 🔥 corrigir apenas quando necessário
    mask_corrigir = (
        df['total_spent'].isna() |
        (df['total_spent'] != df['quantity'] * df['price_per_unit'])
    )

    qtd = mask_corrigir.sum()

    df.loc[mask_corrigir, 'total_spent'] = (
        df['quantity'] * df['price_per_unit']
    )

    logging.info(f"{qtd} valores de Total Spent corrigidos")

    return df


def tratar_nulos(df):
    logging.info("Tratando valores nulos...")

    # 🔴 colunas críticas → remover
    df = df[df['quantity'].notna()]
    df = df[df['price_per_unit'].notna()]

    # 🟡 categóricas → preencher
    for col in ['item', 'payment_method', 'location']:
        if col in df.columns:
            df[col] = df[col].fillna("desconhecido")

    # 🔴 data → geralmente remover
    df = df[df['transaction_date'].notna()]

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

    # 🔴 valores negativos
    neg_qtd = (df['quantity'] < 0).sum()
    neg_price = (df['price_per_unit'] < 0).sum()
    neg_total = (df['total_spent'] < 0).sum()

    logging.info(f"Quantity negativa: {neg_qtd}")
    logging.info(f"Price negativo: {neg_price}")
    logging.info(f"Total negativo: {neg_total}")

    df = df[df['quantity'] >= 0]
    df = df[df['price_per_unit'] >= 0]
    df = df[df['total_spent'] >= 0]

    # 🔴 valores absurdos (opcional)
    high_total = (df['total_spent'] > 1000).sum()
    logging.info(f"Total muito alto: {high_total}")

    df = df[df['total_spent'] <= 1000]

    # 🔴 inconsistência (garantia final)
    inconsistentes = (
        df['total_spent'] != df['quantity'] * df['price_per_unit']
    ).sum()

    logging.info(f"Inconsistências restantes: {inconsistentes}")

    depois = len(df)

    logging.info(f"Total removido na validação: {antes - depois}")

    return df


def pipeline(df):

    log_section("🚀 INÍCIO DA PIPELINE")

    info_basic(df)
    infos_auxiliares(df)

    df = padronizar_colunas(df)
    df = padronizar_textos(df)
    df = tratar_valores_invalidos(df)
    df = converter_tipos(df)
    df = tratar_nulos(df)
    df = tratar_duplicados(df)
    df = validar_dados(df)

    info_basic(df)

    log_section("✅ PIPELINE FINALIZADA")

    return df


if __name__ == "__main__":

    df = pd.read_csv("Example_2/data/dirty_cafe_sales.csv")

    df_limpo = pipeline(df)

    relatorio_qualidade(df_limpo, df)

    df_limpo.to_csv("Example_2/data/dados_limpos.csv", index=False)
