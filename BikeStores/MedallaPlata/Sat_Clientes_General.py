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
          CREATE TABLE IF NOT EXISTS Sat_Clientes_General
          (
              Hk_Clientes BINARY NOT NULL COMMENT 'Llave Hash creada por la llave de negocio de la tabla Hub, identifica de forma única a cada llave de negocio en la tabla'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Registra el momento exacto en el tiempo en que el registro de la tabla se guardó en la misma'
              ,Hk_Diff BINARY NOT NULL COMMENT 'Llave Hash que lleva el control de cambios de las variables del Cliente'
              ,Nombre STRING NOT NULL COMMENT 'Nombre del Cliente'
              ,Apellido STRING NOT NULL COMMENT 'Apellido del Cliente'
              ,CalleUbicacion STRING NOT NULL COMMENT 'Calle donde vive el Cliente'
              ,CiudadUbicacion STRING NOT NULL COMMENT 'Ciudad donde vive el Cliente'
              ,CodigoEstadoUbicacion STRING NOT NULL COMMENT 'Codigo del Estado donde vive el Cliente'
              ,CodigoPostalUbicacion INT NOT NULL COMMENT 'Codigo Postal donde vive el Cliente'
              ,FuenteDatos STRING NOT NULL COMMENT 'Origen del Datos residente en la Medalla de Bronce'
          )
          USING DELTA
          COMMENT 'Tabla de tipo Satelite para los Clientes con los datos Generales del Cliente'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (FechaRegistro)
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
                    B.Hk_Clientes
                    ,CURRENT_TIMESTAMP() AS FechaRegistro 
                    ,B.Hk_Diff
                    ,B.Nombre
                    ,B.Apellido
                    ,B.CalleUbicacion
                    ,B.CiudadUbicacion
                    ,B.CodigoEstadoUbicacion
                    ,B.CodigoPostalUbicacion
                    ,'abfss://{contenedorRegional}@{lagoDatos}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/BikeStores/sales/customers/' AS FuenteDatos
                FROM 
                    (
                    SELECT 
                        A.Nombre 
                        ,A.Apellido
                        ,A.CalleUbicacion
                        ,A.CiudadUbicacion
                        ,A.CodigoEstadoUbicacion
                        ,A.CodigoPostalUbicacion
                        ,CAST(SHA2(CAST(A.customer_id AS STRING), 256) AS BINARY) AS Hk_Clientes 
                        ,CAST(SHA2((CAST(A.customer_id AS STRING) || A.Nombre || A.Apellido || A.CalleUbicacion || A.CiudadUbicacion || A.CodigoEstadoUbicacion || CAST(A.CodigoPostalUbicacion AS STRING)), 512) AS BINARY) AS Hk_Diff 
                    FROM 
                        (
                        SELECT 
                            customer_id 
                            ,COALESCE(NULLIF(TRIM(first_name), ''), 'No Registra') AS Nombre
                            ,COALESCE(NULLIF(TRIM(last_name), ''), 'No Registra') AS Apellido 
                            ,COALESCE(NULLIF(TRIM(street), ''), 'No Registra') AS CalleUbicacion
                            ,COALESCE(NULLIF(TRIM(city), ''), 'No Registra') AS CiudadUbicacion
                            ,COALESCE(NULLIF(TRIM(state), ''), 'N/R') AS CodigoEstadoUbicacion
                            ,COALESCE(CASE WHEN zip_code <= 0 THEN NULL ELSE zip_code END , -1) AS CodigoPostalUbicacion
                        FROM 
                            tmp_ParquetCliente
                        WHERE 
                            customer_id IS NOT NULL
                        ) AS A
                    ) AS B 
                    LEFT JOIN 
                    (
                    SELECT 
                        D.Hk_Clientes
                        ,D.HK_Diff
                    FROM 
                        (
                        SELECT 
                            HK_Clientes 
                            ,HK_Diff
                            ,ROW_NUMBER()OVER(PARTITION BY HK_Clientes ORDER BY FechaRegistro DESC) AS RegistroReciente
                        FROM 
                            Sat_Clientes_General
                        ) AS D
                    WHERE 
                        D.RegistroReciente = 1
                    ) AS C 
                        ON B.Hk_Clientes = C.Hk_Clientes AND B.Hk_Diff = C.HK_Diff 
                WHERE 
                    C.Hk_Clientes IS NULL 
                    AND C.Hk_Diff IS NULL
          """)

if not dfRegistrosNuevos.isEmpty():
    dfDeltaSatClientes = DeltaTable.forName(spark, f"{catalogoDataVault}.{contenedorRegional}.Sat_Clientes_General")

    dfDeltaSatClientes.alias("A").merge(
        dfRegistrosNuevos.alias("B")
        ,"A.Hk_Clientes = B.Hk_Clientes AND A.Hk_Diff = B.HK_Diff"
    ).whenNotMatchedInsertAll().execute()


