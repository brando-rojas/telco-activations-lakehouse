# Databricks notebook source
# MAGIC %md
# MAGIC # 06d · Tarea del job: control de calidad
# MAGIC Falla (y hace fallar el job) si el último día procesado tiene demasiadas filas descartadas por las expectativas, o si no llegó nada.
# MAGIC Para practicar *Repair run* (E6.5), baja `umbral_descartes` a 0 y vuelve a subirlo.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("umbral_descartes", "0.05")
umbral = float(dbutils.widgets.get("umbral_descartes"))

bronze = spark.table("gold.audit_pre_bronze").count()
silver = spark.table("gold.audit_pre_silver").count()
descarte = 1 - silver / bronze if bronze else 1.0
print(f"bronze {bronze:,} · silver {silver:,} · descartado {descarte:.2%} (umbral {umbral:.2%})")

if bronze == 0:
    raise Exception("No llegó ningún dato")
if descarte > umbral:
    raise Exception(f"Descarte {descarte:.2%} supera el umbral {umbral:.2%}")
dbutils.notebook.exit(f"OK descarte {descarte:.2%}")

