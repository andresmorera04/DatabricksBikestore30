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
diasCargar = dfParametros.filter("Clave == 'DiasCargarBorrar'").first().Valor
lagoDatos = dfParametros.filter("Clave == 'LagoDatos'").first().Valor
medallaBronce = dfParametros.filter("Clave == 'MedallaBronce'").first().Valor
contenedorRegional = dfParametros.filter("Clave == 'ContenedorLagoDatosReg'").first().Valor
catalogoDataVault = dfParametros.filter("Clave == 'DatabricksCatalogoDataVault'").first().Valor

spark.sql(f"USE {catalogoDataVault}.{contenedorRegional}")

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Sat_Clientes_Telefonos 
          (
              Hk_Clientes BINARY NOT NULL COMMENT 'Llave Hash creada por la llave de negocio de la tabla Hub, identifica de forma única a cada llave de negocio en la tabla'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Registra el momento exacto en el tiempo en que el registro de la tabla se guardó en la misma'
              ,Hk_Diff BINARY NOT NULL COMMENT 'Llave Hash que lleva el control de cambios de las variables del Cliente'
              ,CodigoPais STRING NOT NULL COMMENT 'Codigo de Pais del Número de Telefono'
              ,NumeroTelefono INT NOT NULL COMMENT 'Número de Telefono del Cliente'
              ,FuenteDatos STRING NOT NULL COMMENT 'Origen del Datos residente en la Medalla de Bronce'
          )
          USING DELTA
          COMMENT 'Tabla de tipo Satelite para los Clientes con los datos Generales del Cliente'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (FechaRegistro)
          """)

fechaFin = datetime.strptime(fechaProceso, "%Y-%m-%d")
fechaInicio = fechaFin - timedelta(days=int(diasCargar))
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
                A.Hk_Clientes
                ,current_date() AS FechaRegistro
                ,A.codigo_pais AS CodigoPais 
                ,A.numero_telefono AS NumeroTelefono 
                ,A.hk_diff AS Hk_Diff
                ,'abfss://{contenedorRegional}@{lagoDatos}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/BikeStores/sales/customers/' AS FuenteDatos
                FROM 
                ( 
                SELECT 
                    CAST(sha2(
                    CAST((CASE 
                    WHEN ISNULL(customer_id) = TRUE THEN -1
                    ELSE customer_id 
                    END) AS STRING)
                    ,256) AS BINARY) AS hk_clientes
                    ,SUBSTRING(
                        (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)
                    ,1, charindex(')', (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)) ) AS codigo_pais 
                    ,CASE 
                    WHEN ISNULL(CAST(
                    REPLACE(
                    SUBSTRING((CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)
                    ,(charindex(')', (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)) + 1)
                    ,25), '-', '') 
                    AS INT ) 
                    ) = TRUE THEN -1 
                    ELSE  CAST(
                    REPLACE(
                    SUBSTRING((CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)
                    ,(charindex(')', (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)) + 1)
                    ,25), '-', '') 
                    AS INT )
                    END AS numero_telefono
                    ,CAST(sha2(
                    (CAST((CASE 
                        WHEN ISNULL(customer_id) = TRUE THEN -1
                        ELSE customer_id 
                    END) AS STRING) || 
                    SUBSTRING(
                        (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)
                    ,1, charindex(')', (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)) ) || REPLACE(
                    SUBSTRING((CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)
                    ,(charindex(')', (CASE 
                        WHEN isnull(phone) = TRUE THEN ''
                        ELSE phone
                    END)) + 1)
                    ,25), '-', '') )
                    , 512)
                    AS BINARY) AS hk_diff
                FROM 
                    tmp_ParquetCliente 
                WHERE 
                    phone IS NOT NULL 
                    AND phone != 'NULL'
                ) AS A 
                LEFT JOIN Sat_Clientes_Telefonos AS B 
                    ON A.hk_clientes = B.hk_clientes AND A.hk_diff = B.hk_diff
                WHERE 
                B.hk_clientes IS NULL 
                AND B.hk_diff IS NULL 
                ;
          """)

if not dfRegistrosNuevos.isEmpty():
    dfDeltaSatClientesTelefonos = DeltaTable.forName(spark, f"{catalogoDataVault}.{contenedorRegional}.Sat_Clientes_Telefonos")

    dfDeltaSatClientesTelefonos.alias("A").merge(
        dfRegistrosNuevos.alias("B")
        ,"A.Hk_Clientes = B.Hk_Clientes AND A.Hk_Diff = B.HK_Diff"
    ).whenNotMatchedInsertAll().execute()


