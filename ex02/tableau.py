import pandas as pd

# Fonction pour tester si une valeur est une date/datetime
def is_datetime(s):
    try:
        pd.to_datetime(s)
        return True
    except:
        return False

def detect_pg_type(col_values, is_first_col=False):
    col_str = col_values.astype(str)

    # Vérifier int
    if all(col_str.str.isdigit()):
        return "INT"
    # Vérifier float
    elif all(col_str.str.replace('.', '', 1).str.isdigit()):
        return "FLOAT"
    # Vérifier booléen
    elif all(col_str.str.lower().isin(['true', 'false'])):
        return "BOOLEAN"
    # Vérifier datetime
    elif all(col_str.apply(is_datetime)):
        return "TIMESTAMP" if is_first_col else "DATE"
    # Sinon texte
    else:
        max_len = col_str.apply(len).max()
        return f"VARCHAR({max_len})"


# Fonction principale pour générer la requête SQL
def generate_create_table_sql(csv_path):
    df = pd.read_csv(csv_path)
    table_name = csv_path.split('/')[-1].replace('.csv','')  # nom table = nom fichier CSV sans extension

    # Boucle sur toutes les colonnes pour détecter leur type
    columns_sql = []
    for i, col in enumerate(df.columns):
        pg_type = detect_pg_type(df[col])
        # S’assurer que la première colonne est TIMESTAMP
        if i == 0:
            pg_type = "TIMESTAMP"
        columns_sql.append(f"{col} {pg_type}")

    # Générer la requête SQL finale
    create_table_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n);"

    print("Requête SQL générée :\n")
    print(create_table_sql)

# Exemple d’utilisation
def main():
    generate_create_table_sql("customer/data_2025_nov.csv")

if __name__ == "__main__":
    main()
