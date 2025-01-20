# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import *

conexionConfiguracion = dbutils.secrets.get(scope="amsedatabricks30", key="secrazuresqljdbcintegrator")

dfParametros = spark.read.format("jdbc").options(
    url = conexionConfiguracion, dbtable = "dbo.Parametros", driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

fechaProceso = dfParametros.filter("Clave == 'FechaProceso'").first().Valor
disCargar = dfParametros.filter("Clave == 'DiasCargarBorrar'").first().Valor
lagoDatos = dfParametros.filter("Clave == 'LagoDatos'").first().Valor
medallaBronce = dfParametros.filter("Clave == 'MedallaBronce'").first().Valor
contenedorRegional = dfParametros.filter("Clave == 'ContenedorLagoDatosReg'").first().Valor
catalogoDataVault = dfParametros.filter("Clave == 'DatabricksCatalogoDataVault'").first().Valor

spark.sql(f"USE {catalogoDataVault}.{contenedorRegional}")

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Hub_Clientes 
          (
              Hk_Clientes BINARY NOT NULL COMMENT 'Llave Hash creada por la llave de negocio de la tabla Hub, identifica de forma única a cada llave de negocio en la tabla'
              ,Bk_IdCliente INT NOT NULL COMMENT 'Llave de negocio que identifica de forma única a cada cliente'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Registra el momento exacto en el tiempo en que el registro de la tabla se guardó en la misma'
              ,FuenteDatos STRING NOT NULL COMMENT 'Origen del Datos residente en la Medalla de Bronce'
          )
          USING DELTA
          COMMENT 'Tabla de tipo Hub para los Clientes, contiene todas las llaves de negocio de los Clientes'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (Bk_IdCliente)
          """)

fechaFin = datetime.strptime(fechaProceso, "%Y-%m-%d")
fechaInicio = fechaFin - timedelta(days=int(disCargar))
hora = time(23, 59, 59)
fechaFin = datetime.combine(fechaFin, hora)
rutaArchivoParquet = f"abfss://{contenedorRegional}@{lagoDatos}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/BikeStores/sales/customers/"


# COMMAND ----------

dfParquetCliente = spark.read.format("parquet").load(rutaArchivoParquet)
dfParquetCliente = dfParquetCliente.withColumn("FechaArchivos", to_date(concat(col("`año`").cast("string"), lit("-"), col("mes").cast("string"), lit("-"), col("dia").cast("string")), "yyyy-M-d"))
dfParquetCliente = dfParquetCliente.filter(f"FechaArchivos >= '{str(fechaInicio)[0:10]}' and FechaArchivos <= '{str(fechaFin)[0:10]}'")
fechaArchivoReciente = dfParquetCliente.select(max("FechaArchivos")).first()[0]
dfParquetCliente = dfParquetCliente.filter(f"FechaArchivos == '{fechaArchivoReciente}'")
dfParquetCliente.createOrReplaceTempView("tmp_ParquetCliente")

# COMMAND ----------

dfRegistrosNuevos = spark.sql(f"""
                SELECT 
                    CAST(SHA2(CAST(A.Bk_IdCliente AS STRING), 256) AS BINARY) AS Hk_Clientes 
                    ,A.Bk_IdCliente
                    ,CURRENT_TIMESTAMP() AS FechaRegistro
                    ,'abfss://{contenedorRegional}@{lagoDatos}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/BikeStores/sales/customers/' AS FuenteDatos
                FROM 
                    (
                    SELECT 
                        customer_id AS Bk_IdCliente 
                    FROM 
                        tmp_ParquetCliente
                    WHERE 
                        customer_id IS NOT NULL 
                    GROUP BY 
                        customer_id
                    ) AS A
                    LEFT JOIN Hub_Clientes AS B 
                        ON A.Bk_IdCliente = B.Bk_IdCliente
                WHERE 
                    B.Bk_IdCliente IS NULL
          """)

if not dfRegistrosNuevos.isEmpty():
    dfDeltaHubClientes = DeltaTable.forName(spark, f"{catalogoDataVault}.{contenedorRegional}.Hub_Clientes")

    dfDeltaHubClientes.alias("A").merge(
        dfRegistrosNuevos.alias("B")
        ,"A.Bk_IdCliente = B.Bk_IdCliente"
    ).whenNotMatchedInsertAll().execute()


