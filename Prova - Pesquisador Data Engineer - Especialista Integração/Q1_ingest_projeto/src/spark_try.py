from typing import Optional, Dict, Any

def try_csv_rowcount_with_spark(file_path: str) -> Optional[int]:
    """
    Tenta inicializar Spark e contar linhas do CSV (ignora header). Retorna None se falhar.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("Q1RowCount").getOrCreate()
        df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
        cnt = df.count()
        spark.stop()
        return int(cnt)
    except Exception:
        return None
