import logging


def infos_auxiliares(df):

    with open("caminho.txt", "w", encoding="utf-8") as f:
        f.write("=== RELATÓRIO DE DADOS ===\n\n")

        for col in df.columns:
            n_unicos = df[col].nunique(dropna=True)
            pct_nulos = df[col].isna().mean() * 100

            f.write(f"Coluna: {col}\n")
            f.write(f"Tipo: {df[col].dtype}\n")
            f.write(f"Qtd únicos: {n_unicos}\n")
            f.write(f"% nulos: {pct_nulos:.2f}%\n")

            if n_unicos <= 15:
                valores = df[col].dropna().unique()
                f.write(f"Valores únicos: {valores}\n")
            else:
                top = df[col].value_counts().head(5)
                f.write(f"Top 5 valores:\n{top}\n")

            f.write("-" * 40 + "\n")


def relatorio_qualidade(df, df_original=None, path="caminho.txt"):

    logger = logging.getLogger("relatorio")
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(path, mode="w", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # evita duplicação de logs
    if not logger.handlers:
        logger.addHandler(handler)

    logger.info("===== RELATÓRIO DE QUALIDADE =====")

    total_linhas = len(df)

    if df_original is not None:
        total_original = len(df_original)
        removidos = total_original - total_linhas
        pct = removidos / total_original

        logger.info(f"Linhas originais: {total_original}")
        logger.info(f"Linhas finais: {total_linhas}")
        logger.info(f"Removidos: {removidos} ({pct:.2%})")

    logger.info("\n===== NULOS =====")
    logger.info(df.isna().mean())

    logger.info("\n===== DESCRIBE =====")
    logger.info(df.describe())

    logger.info("\n===== FIM DO RELATÓRIO =====")
