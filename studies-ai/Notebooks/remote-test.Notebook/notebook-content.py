# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6e1f0e8f-9477-434d-9ff3-34bfb94f7b83",
# META       "default_lakehouse_name": "olist_brazillian",
# META       "default_lakehouse_workspace_id": "a2e2c08a-bec5-448a-8d2e-14304dd272e9",
# META       "known_lakehouses": [
# META         {
# META           "id": "6e1f0e8f-9477-434d-9ff3-34bfb94f7b83"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip -q install kaggle

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================
# FABRIC NOTEBOOK (PySpark)
# Kaggle Olist -> OneLake Files (abfss://.../Files/<pasta>) -> Lakehouse tables
# Lakehouse: olist_brazillian
# Schema novo: raw_olist
# =========================================

import os, re, json, shutil, builtins
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import functions as F

# ----------------------------
# CONFIG
# ----------------------------
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

LAKEHOUSE = "olist_brazillian"
TARGET_SCHEMA = "raw_olist"

# ABFSS base que você passou (até /Files)
DEST_FILES_ABFSS_ROOT = "abfss://a2e2c08a-bec5-448a-8d2e-14304dd272e9@onelake.dfs.fabric.microsoft.com/6e1f0e8f-9477-434d-9ff3-34bfb94f7b83/Files"
FOLDER_NAME = "olist_kaggle_brazilian_ecommerce"  # pasta nova em Files

DEST_FOLDER_ABFSS = f"{DEST_FILES_ABFSS_ROOT}/{FOLDER_NAME}"

BASE_DIR = Path("/tmp/olist_kaggle")
DATA_DIR = BASE_DIR / "data"

# ----------------------------
# NotebookUtils FS (fallback para mssparkutils)
# ----------------------------
try:
    fs = notebookutils.fs
except NameError:
    from notebookutils import mssparkutils  # alguns runtimes expõem assim
    fs = mssparkutils.fs

# ----------------------------
# 0) Credenciais Kaggle via Variable Library
# ----------------------------
k_user = notebookutils.variableLibrary.get("$(/**/Variables/KAGGLE_USERNAME)")
k_key  = notebookutils.variableLibrary.get("$(/**/Variables/KAGGLE_KEY)")

k_user = (k_user or "").strip()
k_key  = (k_key  or "").strip()

if not k_user or not k_key:
    raise ValueError("KAGGLE_USERNAME/KAGGLE_KEY vieram vazios da Variable Library. Verifique o Value Set ativo.")

# ENV (Kaggle reconhece)
os.environ["KAGGLE_USERNAME"] = k_user
os.environ["KAGGLE_KEY"]      = k_key

# kaggle.json no path padrão
kaggle_home = Path.home() / ".kaggle"
kaggle_home.mkdir(parents=True, exist_ok=True)
(kaggle_home / "kaggle.json").write_text(json.dumps({"username": k_user, "key": k_key}), encoding="utf-8")
try:
    os.chmod(str(kaggle_home / "kaggle.json"), 0o600)
except Exception:
    pass
os.environ["KAGGLE_CONFIG_DIR"] = str(kaggle_home)

# Fix exit (Fabric às vezes não tem)
if not hasattr(builtins, "exit"):
    def _exit(code=0):
        raise SystemExit(code)
    builtins.exit = _exit

# ----------------------------
# 1) Download + Unzip (local)
# ----------------------------
if BASE_DIR.exists():
    shutil.rmtree(BASE_DIR, ignore_errors=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

api = KaggleApi()
api.authenticate()
api.dataset_download_files(KAGGLE_DATASET, path=str(DATA_DIR), unzip=True)

# ----------------------------
# 2) Cria pasta nova em Files (ABFSS)
# ----------------------------
fs.mkdirs(DEST_FOLDER_ABFSS)

# ----------------------------
# 3) Copia CSVs locais -> OneLake Files (ABFSS)
# ----------------------------
csv_files = sorted(DATA_DIR.rglob("*.csv"))
if not csv_files:
    raise FileNotFoundError(f"Nenhum CSV encontrado em {DATA_DIR}. O download pode ter falhado.")

copied = []
for p in csv_files:
    src = f"file:{str(p)}"
    dst = f"{DEST_FOLDER_ABFSS}/{p.name}"
    fs.cp(src, dst)
    copied.append(dst)

print(f"✅ CSVs copiados para: {DEST_FOLDER_ABFSS}")
print("Exemplo:", copied[0])

# ----------------------------
# 4) Cria schema no Lakehouse e grava tabelas Delta lendo do ABFSS
# ----------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE}.{TARGET_SCHEMA}")

def to_snake(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name

def normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, to_snake(c))
    return df

TS_HINTS = {
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "shipping_limit_date",
    "review_creation_date",
    "review_answer_timestamp",
}
def cast_known_timestamps(df):
    cols = set(df.columns)
    for c in (TS_HINTS & cols):
        df = df.withColumn(c, F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"))
    return df

loaded = []
for abfss_file in copied:
    file_name = abfss_file.split("/")[-1]
    table_name = to_snake(file_name.replace(".csv", ""))
    full_table = f"{LAKEHOUSE}.{TARGET_SCHEMA}.{table_name}"

    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(abfss_file))

    df = normalize_columns(df)
    df = cast_known_timestamps(df)

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table))

    loaded.append((full_table, df.count()))

print("✅ Tabelas carregadas no Lakehouse:")
for t, n in loaded:
    print(f" - {t} (linhas={n})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
