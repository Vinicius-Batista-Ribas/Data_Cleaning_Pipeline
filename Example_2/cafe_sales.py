import pandas as pd
import numpy as np
import logging
from datetime import datetime

# =========================
# CONFIG LOG
# =========================


def configurar_log():
    logging.basicConfig(
        filename="Example_2/pipeline_log.txt",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8"
    )


def log(msg):
    print(msg)
    logging.info(msg)


# =========================
# INFO INICIAL
# =========================
def info_basic(df):
    log(f"\n===== INFO BÁSICA =====")
    log(f"HEAD:\n{df.head(10)}")
    log(f"Shape: {df.shape}")
    log(f"Describe:\n{df.describe()}")
    log(f"Tipos:\n{df.dtypes}")
    log(f"Colunas: {list(df.columns)}")
    log(f"Nulos:\n{df.isnull().sum()}")
    log(f"Duplicados: {df.duplicated().sum()}")


# =========================
# VALIDANDO DADOS
# =========================
def tratar_dados(df):

    return df


# =========================
# TRATAR NULOS
# =========================
def tratar_nulos(df):
    log("\n===== TRATAMENTO DE NULOS =====")

    total_linhas = len(df)

    for col in df.columns:
        qtd_nulos = df[col].isnull().sum()
        perc = (qtd_nulos / total_linhas) * 100

        if qtd_nulos == 0:
            continue

        log(f"Coluna: {col} | Nulos: {qtd_nulos} ({perc:.2f}%)")

        if perc < 5:
            log(f"-> Removendo linhas com nulos em {col}")
            df = df.dropna(subset=[col])
        else:
            log(f"-> ATENÇÃO: {col} tem mais de 5% de nulos. Necessário análise.")

            # 🔧 PERSONALIZAÇÃO AQUI
            if df[col].dtype == 'object':
                log(f"-> Preenchendo com 'DESCONHECIDO'")
                df[col] = df[col].fillna("DESCONHECIDO")
            else:
                media = df[col].mean()
                log(f"-> Preenchendo com média: {media}")
                df[col] = df[col].fillna(media)

    return df


# =========================
# TRATAR DUPLICADOS
# =========================
def tratar_duplicados(df):
    log("\n===== TRATAMENTO DE DUPLICADOS =====")

    duplicados = df.duplicated().sum()
    log(f"Duplicados encontrados: {duplicados}")

    if duplicados > 0:
        df = df.drop_duplicates()
        log("Duplicados removidos")

    return df


# =========================
# PADRONIZAR TEXTOS
# =========================
def padronizar_textos(df):
    log("\n===== PADRONIZAÇÃO DE TEXTOS =====")

    for col in df.select_dtypes(include='object').columns:
        log(f"Padronizando coluna: {col}")

        df[col] = df[col].astype(str).str.strip().str.lower()

        # 🔧 PERSONALIZAÇÃO AQUI
        # Exemplo:
        # df[col] = df[col].str.replace("á", "a")

    return df


# =========================
# PADRONIZAR CATEGORIAS
# =========================
def padronizar_categorias(df):
    log("\n===== PADRONIZAÇÃO DE CATEGORIAS =====")

    # 🔧 PERSONALIZAÇÃO FORTE AQUI
    # Exemplo de mapeamento:
    mapas = {
        # "sexo": {"m": "masculino", "f": "feminino"}
    }

    for col, mapa in mapas.items():
        if col in df.columns:
            log(f"Padronizando categorias da coluna: {col}")
            df[col] = df[col].map(mapa).fillna(df[col])

    return df


# =========================
# VALIDAR DADOS
# =========================
def validar_dados(df):
    log("\n===== VALIDAÇÃO DE DADOS =====")

    # 🔧 PERSONALIZAÇÃO FORTE AQUI
    # Exemplo:
    if "idade" in df.columns:
        invalidos = df[df["idade"] < 0]
        log(f"Idades inválidas: {len(invalidos)}")

        df = df[df["idade"] >= 0]

    return df


# =========================
# PIPELINE PRINCIPAL
# =========================
def pipeline(df):
    configurar_log()

    log("\n=========== INICIO PIPELINE ===========")

    info_basic(df)
    df = tratar_dados(df)
    # df = tratar_nulos(df)
    # df = tratar_duplicados(df)
    # df = padronizar_textos(df)
    # df = padronizar_categorias(df)
    # df = validar_dados(df)

    # info_basic(df)

    # log("\n=========== FIM PIPELINE ===========")

    return df


# =========================
# EXECUÇÃO
# =========================
if __name__ == "__main__":
    # 🔧 ALTERAR AQUI PARA SEU DATASET
    df = pd.read_csv("Example_2\data\dirty_cafe_sales.csv")

    df_tratado = pipeline(df)

    # 🔧 OPCIONAL: salvar resultado
    # df_tratado.to_csv("Example_2\data\dados_tratados.csv", index=False)
