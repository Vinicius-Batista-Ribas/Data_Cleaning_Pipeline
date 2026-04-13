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


def log_section(titulo):
    logging.info("\n" + "="*50)
    logging.info(f"📌 {titulo}")
    logging.info("="*50)

##########################################
# 🔎 ANÁLISE
##########################################


def info_basic(df, titulo="DATAFRAME"):
    log_section(f"ANÁLISE - {titulo}")
    logging.info(f"📏 Shape: {df.shape}")
    logging.info("\n🔍 HEAD:\n%s", df.head(3))
    logging.info("\n📊 DESCRIBE:\n%s", df.describe(include='all'))

##########################################
# 🧹 LIMPEZA PADRÃO
##########################################


def tratar_nulos(df):
    log_section("🧹 TRATAMENTO DE NULOS")

    antes = df.shape[0]
    nulos = df.isnull().sum().sum()

    logging.info(f"🔎 Total de nulos: {nulos}")

    df = df.dropna()

    depois = df.shape[0]

    logging.info(f"📉 Linhas antes: {antes}")
    logging.info(f"📈 Linhas depois: {depois}")
    logging.info(f"❌ Removidas: {antes - depois}")

    return df


def tratar_duplicados(df):
    log_section("🔁 TRATAMENTO DE DUPLICADOS")

    antes = df.shape[0]
    duplicados = df.duplicated().sum()

    logging.info(f"🔁 Duplicados encontrados: {duplicados}")

    df = df.drop_duplicates()

    depois = df.shape[0]

    logging.info(f"📉 Linhas antes: {antes}")
    logging.info(f"📈 Linhas depois: {depois}")
    logging.info(f"❌ Removidas: {antes - depois}")

    return df

##########################################
# 🧩 PADRONIZAÇÃO
##########################################


def padronizar_textos(df):
    log_section("🧩 PADRONIZAÇÃO DE TEXTOS")

    colunas = df.select_dtypes(include=['object']).columns

    for col in colunas:
        valores_antes = df[col].unique()[:5]

        df[col] = df[col].str.strip().str.lower()

        valores_depois = df[col].unique()[:5]

        logging.info(f"\n➡️ Coluna: {col}")
        logging.info(f"Antes: {valores_antes}")
        logging.info(f"Depois: {valores_depois}")

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

    antes = df.shape[0]

    # EXEMPLO:
    # df = df[df["valor"] >= 0]

    depois = df.shape[0]

    logging.info(f"📉 Linhas antes: {antes}")
    logging.info(f"📈 Linhas depois: {depois}")
    logging.info(f"❌ Removidas: {antes - depois}")

    return df

##########################################
# 🚀 PIPELINE
##########################################


def pipeline(df):
    log_section("🚀 INÍCIO DA PIPELINE")

    info_basic(df, "ANTES")

    linhas_iniciais = df.shape[0]

    df = tratar_nulos(df)
    df = tratar_duplicados(df)
    df = padronizar_textos(df)
    df = padronizar_categorias(df)
    df = validar_dados(df)

    linhas_finais = df.shape[0]

    log_section("📊 RESUMO FINAL")

    logging.info(f"📌 Linhas iniciais: {linhas_iniciais}")
    logging.info(f"📌 Linhas finais: {linhas_finais}")
    logging.info(f"📌 Total removido: {linhas_iniciais - linhas_finais}")

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
