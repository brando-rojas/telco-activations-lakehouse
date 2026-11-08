# Databricks notebook source
# MAGIC %md
# MAGIC # 06c · Tarea del job: llegan días nuevos
# MAGIC Recibe el parámetro `dias` del job (E6.5) y copia esa cantidad de días de ACTION_AUDIT prepago a la carpeta de llegadas.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("dias", "3")
dias = int(dbutils.widgets.get("dias"))

ORIGEN = "CM_PRE.ACTION_AUDIT"
DESTINO_LLEGADAS = f"{LLEGADAS}/action_audit_pre"
try:
    ya = {f.name for f in dbutils.fs.ls(DESTINO_LLEGADAS)}
except Exception:
    ya = set()
pendientes = [p for p in archivos_parquet(ORIGEN) if p.rsplit("/", 1)[-1] not in ya]
for p in pendientes[:dias]:
    dbutils.fs.cp(p, f"{DESTINO_LLEGADAS}/{p.rsplit('/', 1)[-1]}")

resultado = f"llegaron {min(dias, len(pendientes))} dias, quedan {max(len(pendientes) - dias, 0)}"
print(resultado)
dbutils.notebook.exit(resultado)

