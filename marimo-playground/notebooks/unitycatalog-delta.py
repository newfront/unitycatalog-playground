import marimo

__generated_with = "0.20.1"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import os
    import marimo as mo
    import getpass
    from delta import configure_spark_with_delta_pip

    from pyspark.sql import SparkSession

    return SparkSession, configure_spark_with_delta_pip


@app.cell
def _():
    unity_catalog_server_url = "http://localhost:8080"

    catalog = 'unity'
    schema = 'default'
    return catalog, unity_catalog_server_url


@app.cell
def _(
    SparkSession,
    catalog,
    configure_spark_with_delta_pip,
    unity_catalog_server_url,
):
    builder = (
        SparkSession.builder
        .appName("DeltaCatalogManagedTables")
        .master("local[*]")    
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{catalog}", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", unity_catalog_server_url)
        .config(f"spark.sql.catalog.{catalog}.token", "")
        .config("spark.sql.defaultCatalog", catalog)
    )

    extra_packages = [
        "io.unitycatalog:unitycatalog-spark_2.13:0.4.0",
    ]

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages
    ).getOrCreate()
    return (spark,)


@app.cell
def _():
    import subprocess
    result = subprocess.run(["java", "-version"], capture_output=True, text=True)
    print(result.stderr)
    return


@app.cell
def _(spark):
    spark.sql("select * from default.marksheet").show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
