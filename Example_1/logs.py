import logging


def infos_auxiliares(df):

    with open("Example_1/logs/infos_auxiliares.txt", "w", encoding="utf-8") as f:
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
