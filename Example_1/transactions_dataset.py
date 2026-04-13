import pandas as pd
from logs import infos_auxiliares
import logging

##########################################
# ⚙️ CONFIG LOG
##########################################

logging.basicConfig(
    filename="Example_1/logs/logs_pipeline.txt",
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


def padronizar_dados(df):

    logging.info("Padronizando textos...")

    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

    logging.info("Padronizando categorias...")

    mapa_card = {
        "vsa": "visa",
        "VISA": "visa",
        "visa": "visa",
        "Visa": "visa",

        "MastCard": "master_card",
        "mastercard": "master_card",
        "master-card": "master_card",
        "master card": "master_card"
    }

    mapa_status = {
        "sucess": "success",
        "succeed": "success",
        "succeed": "success",

        "failed": "fail",
        "fail": "fail",
        "FAIL": "fail"
    }

    mapa_city = {
        "Tehran": "tehran",
        "Tehran": "tehran",
        "TEHRAN": "tehran",
        "tehr@n": "tehran",
        "thran": "tehran",
        "thr": "tehran"
    }

    if "card_type" in df.columns:
        df["card_type"] = df["card_type"].replace(mapa_card)

    if "status" in df.columns:
        df["status"] = df["status"].replace(mapa_status)

    if "city" in df.columns:
        df["city"] = df["city"].replace(mapa_city)

    logging.info("Convertendo dados...")
    df['time'] = pd.to_datetime(df['time'], errors='coerce')

    return df


def tratar_nulos(df):
    logging.info("Tratando valores nulos...")

    total_linhas = len(df)

    for col in df.columns:
        pct_nulos = df[col].isna().mean()

        if pct_nulos == 0:
            continue

        logging.info(f"Coluna: {col} | % nulos: {pct_nulos:.2%}")

        # 🔴 poucos nulos → remover linhas
        if pct_nulos < 0.05:
            antes = len(df)
            df = df[df[col].notna()]
            depois = len(df)

            logging.info(
                f"Removidas {antes - depois} linhas por nulo em {col}")

        # 🟡 muitos nulos → preencher
        else:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("desconhecido")

            elif pd.api.types.is_numeric_dtype(df[col]):
                mediana = df[col].median()
                df[col] = df[col].fillna(mediana)

            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(method="ffill")

            logging.info(f"Nulos preenchidos na coluna {col}")

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


def tratar_amount(df):
    logging.info("Tratando coluna amount...")

    mediana = df.loc[(df['amount'] >= 0) & (
        df['amount'] < 1e9), 'amount'].median()

    # substituir valores inválidos
    mask_invalido = (df['amount'] < 0) | (df['amount'] >= 1e9)

    qtd = mask_invalido.sum()

    df.loc[mask_invalido, 'amount'] = mediana

    logging.info(
        f"{qtd} valores de amount substituídos pela mediana ({mediana})")

    return df


def validar_dados(df):
    logging.info("Validando dados...")

    antes = len(df)

    # regras separadas
    invalid_amount_neg = (df['amount'] < 0).sum()
    invalid_amount_high = (df['amount'] >= 1e9).sum()
    invalid_status = (~df['status'].isin(['success', 'fail'])).sum()

    logging.info(f"Amount negativo: {invalid_amount_neg}")
    logging.info(f"Amount alto: {invalid_amount_high}")
    logging.info(f"Status inválido: {invalid_status}")

    df = df[df['amount'] >= 0]
    df = df[df['amount'] < 1e9]
    df = df[df['status'].isin(['success', 'fail'])]

    depois = len(df)

    logging.info(f"Total removido: {antes - depois}")

    return df


def pipeline(df):

    log_section("🚀 INÍCIO DA PIPELINE")

    info_basic(df)
    infos_auxiliares(df)

    df = padronizar_dados(df)
    df = tratar_nulos(df)
    df = tratar_duplicados(df)
    df = tratar_amount(df)
    df = validar_dados(df)

    info_basic(df)

    log_section("✅ PIPELINE FINALIZADA")

    return df


if __name__ == "__main__":

    df = pd.read_csv("Example_1/data/trx-10k.csv")

    df_limpo = pipeline(df)

    df_limpo.to_csv("Example_2/dados_limpos.csv", index=False)
