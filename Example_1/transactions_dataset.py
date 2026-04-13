import pandas as pd
import logging

##########################################
# ⚙️ CONFIG LOG
##########################################

logging.basicConfig(
    filename="Example_1/logs_pipeline.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

##########################################
# 🔎 ANÁLISE
##########################################


def info_basic(df, titulo="DATAFRAME"):
    logging.info(f"{'='*10} {titulo} {'='*10}")
    logging.info(f"Shape: {df.shape}")
    logging.info(f"\n{df.describe(include='all')}")


##########################################
# 🧹 LIMPEZA
##########################################

def tratar_nulos(df):
    logging.info("Tratando nulos...")

    nulos = df.isnull().sum().sum()
    logging.info(f"Total de nulos: {nulos}")

    df = df.dropna()

    logging.info(f"Linhas apos remocao: {df.shape[0]}")
    return df


def tratar_duplicados(df):
    logging.info("Tratando duplicados...")

    qtd = df.duplicated().sum()
    logging.info(f"Duplicados: {qtd}")

    df = df.drop_duplicates()
    return df


##########################################
# 🧩 PADRONIZAÇÃO
##########################################

def padronizar_textos(df):
    logging.info("Padronizando textos...")

    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

    return df


def padronizar_categorias(df):
    logging.info("Padronizando categorias...")

    mapa_card = {
        "vsa": "visa",
        "mastercard": "master_card",
        "master-card": "master_card",
        "master card": "master_card"
    }

    mapa_status = {
        "sucess": "success",
        "succeed": "success",
        "failed": "fail"
    }

    mapa_city = {
        "tehr@n": "tehran",
        "thran": "tehran",
        "thr": "tehran"
    }

    if "card_type" in df:
        df["card_type"] = df["card_type"].replace(mapa_card)

    if "status" in df:
        df["status"] = df["status"].replace(mapa_status)

    if "city" in df:
        df["city"] = df["city"].replace(mapa_city)

    return df


##########################################
# ✅ VALIDAÇÃO
##########################################

def validar_dados(df):
    logging.info("Validando dados...")

    linhas_antes = df.shape[0]

    if "amount" in df:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df = df[(df["amount"] >= 0) & (df["amount"] <= 1_000_000)]

    if "time" in df:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    if "id" in df:
        logging.info(f"IDs duplicados: {df['id'].duplicated().sum()}")

    linhas_depois = df.shape[0]
    logging.info(
        f"Linhas removidas na validacao: {linhas_antes - linhas_depois}")

    return df


##########################################
# 🚀 PIPELINE
##########################################

def cleaning_pipeline(df):
    logging.info("🚀 Iniciando pipeline")

    linhas_iniciais = df.shape[0]

    info_basic(df, "ANTES")

    df = tratar_nulos(df)
    df = tratar_duplicados(df)
    df = padronizar_textos(df)
    df = padronizar_categorias(df)
    df = validar_dados(df)

    linhas_finais = df.shape[0]

    logging.info(f"Linhas iniciais: {linhas_iniciais}")
    logging.info(f"Linhas finais: {linhas_finais}")
    logging.info(f"Total removido: {linhas_iniciais - linhas_finais}")

    info_basic(df, "DEPOIS")

    logging.info("✅ Pipeline finalizada")

    return df


##########################################
# ▶ EXECUÇÃO
##########################################

if __name__ == "__main__":
    df = pd.read_csv("Example_1/data/trx-10k.csv")

    df_tratado = cleaning_pipeline(df)

    df_tratado.to_csv("Example_1/data/dados_limpos.csv", index=False)
