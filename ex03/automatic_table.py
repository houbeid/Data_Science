from pathlib import Path
import pandas as pd
import psycopg2

def execute_sql(sql_query):
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname="piscineds",
            user="houbeid",
            password="mysecretpassword",
            port=5432
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        cur.close()
        print("Table created successfully.")
    except Exception as e:
        print("SQL ERROR:", e)
    finally:
        if conn:
            conn.close()


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
def generate_create_table_sql(csv_path) -> object:
    df = pd.read_csv(csv_path)
    table_name = csv_path.stem

    # Boucle sur toutes les colonnes pour détecter leur type
    columns_sql = []
    for i, col in enumerate(df.columns):
        pg_type = detect_pg_type(df[col])
        # S’assurer que la première colonne est TIMESTAMP
        if i == 0:
            pg_type = "TIMESTAMP"
        columns_sql.append(f"{col} {pg_type}")

    # Générer la requête SQL finale
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n);"
    return create_table_sql


def automatise(path_str: str):
    folder = Path(path_str)  # ton dossier contenant les CSV

    for csv_file in folder.glob("*.csv"):
        table_sql = generate_create_table_sql(csv_file)
        execute_sql(table_sql)


def main():
    automatise("customer")

if __name__ == "__main__":
    main()