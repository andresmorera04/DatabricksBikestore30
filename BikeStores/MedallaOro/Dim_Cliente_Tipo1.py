# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import *

conexionConfiguracion = dbutils.secrets.get(scope="amsedatabricks30", key="secrazuresqljdbcintegrator")

dfParametros = spark.read.format("jdbc").options(
    url = conexionConfiguracion, dbtable = "dbo.Parametros", driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver", connectTimeout="60").load()

fechaProceso = dfParametros.filter("Clave == 'FechaProceso'").first().Valor
diasCargar = dfParametros.filter("Clave == 'DiasCargarBorrar'").first().Valor
lagoDatos = dfParametros.filter("Clave == 'LagoDatos'").first().Valor
catalogoDataVault = dfParametros.filter("Clave == 'DatabricksCatalogoDataVault'").first().Valor
esquemaDataVaultReg = "reg"
catalogoDwh = dfParametros.filter("Clave == 'DatabricksCatalogoDwh'").first().Valor
esquemaDwhReg = "reg"

spark.sql(f"USE {catalogoDwh}.{esquemaDwhReg}")

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Dim_Cliente 
          (
              DimClienteId INT NOT NULL COMMENT 'Llave Subrrogada del Cliente que permite la relación entre las diferentes tablas de Hechos y Dimensiones asociadas a esta dimensión'
              ,LlaveNegocioIdCliente INT NOT NULL COMMENT 'Llave de Negocio transversal que permite la relación y la intersección de datos entre la Dimensión y las tablas de la zonas de Plata y Bronce. Además, la Llave de Negocio Genera la llave de la Dimensión'
              ,Nombre STRING NOT NULL COMMENT 'Nombre del Cliente'
              ,Apellido STRING NOT NULL COMMENT 'Apellidos del Cliente'
              ,CalleUbicacion STRING NOT NULL COMMENT 'Calle donde se ubica el Cliente'
              ,CiudadUbicacion STRING NOT NULL COMMENT 'Ciudad donde se ubica el Cliente'
              ,CodigoEstadoUbicacion STRING NOT NULL COMMENT 'Código del Estado donde se ubica el Cliente'
              ,CodigoPostalUbicacion INT NOT NULL COMMENT 'Código Postal de donde se ubica el Cliente'
              ,CodigoPaisTelefono STRING NOT NULL COMMENT 'Código del País del número de telefono del Cliente'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Fecha y Hora en que se registra la tupla'
          )
          USING DELTA
          COMMENT 'Tabla Dimensión de Cliente de Tipo 1, contiene datos cualitativos del Cliente para un Modelo de Data Warehouse'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (DimClienteId)
          """)

fechaFin = datetime.strptime(fechaProceso, "%Y-%m-%d")
fechaInicio = fechaFin - timedelta(days=int(diasCargar))
hora = time(23, 59, 59)
fechaFin = datetime.combine(fechaFin, hora)



# COMMAND ----------

# Procedemos a generar la lógica respectiva para tranformar y cargar los datos de una dimension tipo 1

# agregamos el registro dummy validando si el mismo existe o no existe
existeDummy = spark.sql("SELECT COUNT(*) AS existe FROM dim_cliente WHERE DimClienteId = -1").collect()[0]["existe"]

if existeDummy == 0 :
    spark.sql("""
              INSERT INTO dim_cliente (DimClienteId, LlaveNegocioIdCliente, Nombre, Apellido, CalleUbicacion, CiudadUbicacion, CodigoEstadoUbicacion, CodigoPostalUbicacion, CodigoPaisTelefono, FechaRegistro)
              VALUES (-1, -1, 'No Definido', 'No Definido', 'No Definido', 'No Definido', '(N/D)',  -1, '(N/D)', current_timestamp())
              """)

# obtenemos el id maximo de la dimension
idMaximo = spark.sql("SELECT CASE WHEN MAX(DimClienteId) IS NULL THEN 0 ELSE MAX(DimClienteId) END AS DimClienteId FROM dim_cliente WHERE DimClienteId >= 1").collect()[0]["DimClienteId"]

# Obtener los Existentes con cambios 

dfDatosNuevosExistentes = spark.sql(f"""
                                    SELECT 
                                    dim.DimClienteId 
                                    ,dim.LlaveNegocioIdCliente AS LlaveNegocioIdCliente 
                                    ,CASE WHEN plata.nombre IS NULL THEN dim.nombre ELSE plata.nombre END AS Nombre
                                    ,CASE WHEN plata.apellido IS NULL THEN dim.apellido ELSE plata.apellido END AS Apellido
                                    ,CASE WHEN plata.CalleUbicacion IS NULL THEN dim.CalleUbicacion ELSE plata.CalleUbicacion END AS CalleUbicacion
                                    ,CASE WHEN plata.CiudadUbicacion IS NULL THEN dim.CiudadUbicacion ELSE plata.CiudadUbicacion END AS CiudadUbicacion
                                    ,CASE WHEN plata.CodigoEstadoUbicacion IS NULL THEN dim.CodigoEstadoUbicacion ELSE plata.CodigoEstadoUbicacion END AS CodigoEstadoUbicacion 
                                    ,CASE WHEN plata.CodigoPostalUbicacion IS NULL THEN dim.CodigoPostalUbicacion ELSE plata.CodigoPostalUbicacion END AS CodigoPostalUbicacion 
                                    ,CASE WHEN plata.CodigoPais IS NULL THEN dim.CodigoPaisTelefono ELSE plata.CodigoPais END AS CodigoPaisTelefono
                                    ,current_timestamp() AS FechaRegistro 
                                    FROM 
                                    (
                                    SELECT 
                                        A.Bk_IdCliente
                                        ,B.nombre
                                        ,B.apellido
                                        ,D.CodigoPais
                                        ,B.CalleUbicacion
                                        ,B.CiudadUbicacion
                                        ,B.CodigoEstadoUbicacion
                                        ,B.CodigoPostalUbicacion
                                    FROM 
                                        {catalogoDataVault}.{esquemaDataVaultReg}.Hub_Clientes AS A 
                                        INNER JOIN 
                                        (
                                        SELECT 
                                            B2.hk_clientes
                                            ,B2.nombre 
                                            ,B2.apellido
                                            ,B2.CalleUbicacion
                                            ,B2.CiudadUbicacion
                                            ,B2.CodigoEstadoUbicacion
                                            ,B2.CodigoPostalUbicacion
                                        FROM 
                                            (
                                            SELECT 
                                                hk_clientes
                                                ,nombre 
                                                ,apellido
                                                ,CalleUbicacion
                                                ,CiudadUbicacion
                                                ,CodigoEstadoUbicacion
                                                ,CodigoPostalUbicacion
                                                ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY FechaRegistro DESC) AS desduplicador
                                            FROM 
                                                {catalogoDataVault}.{esquemaDataVaultReg}.Sat_Clientes_General 
                                            ) AS B2
                                        WHERE 
                                            B2.desduplicador = 1
                                        ) AS B 
                                        ON A.hk_clientes = B.hk_clientes
                                        INNER JOIN 
                                        (
                                        SELECT 
                                            D2.hk_clientes
                                            ,D2.CodigoPais
                                        FROM 
                                            (
                                            SELECT 
                                            hk_clientes
                                            ,CASE 
                                                WHEN CodigoPais = '' THEN '(N/D)'
                                                ELSE CodigoPais
                                            END CodigoPais
                                            ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY FechaRegistro DESC) AS desduplicador
                                            FROM 
                                            {catalogoDataVault}.{esquemaDataVaultReg}.Sat_Clientes_Telefonos 
                                            ) AS D2 
                                        WHERE 
                                            D2.desduplicador = 1
                                        ) AS D 
                                        ON A.hk_clientes = D.hk_clientes
                                    ) AS plata 
                                    INNER JOIN {catalogoDwh}.{esquemaDwhReg}.Dim_Cliente AS dim 
                                        ON plata.Bk_IdCliente = dim.LlaveNegocioIdCliente
                                    WHERE 
                                    (dim.nombre != plata.nombre)
                                    OR (dim.apellido != plata.apellido)
                                    OR (dim.CodigoPaisTelefono != plata.CodigoPais)
                                    OR (dim.CalleUbicacion != plata.CalleUbicacion)
                                    OR (dim.CiudadUbicacion != plata.CiudadUbicacion)
                                    OR (dim.CodigoEstadoUbicacion != plata.CodigoEstadoUbicacion)
                                    OR (dim.CodigoPostalUbicacion != plata.CodigoPostalUbicacion)
                                    ;
                                    """)

# Obtener los nuevos 
dfTempNuevos = spark.sql(f"""
                        SELECT 
                        (ROW_NUMBER()OVER(ORDER BY plata.Bk_IdCliente ASC) + {str(idMaximo)} ) AS DimClienteId 
                        ,plata.Bk_IdCliente AS LlaveNegocioIdCliente 
                        ,plata.nombre AS Nombre
                        ,plata.apellido AS Apellido
                        ,plata.CalleUbicacion AS CalleUbicacion
                        ,plata.CiudadUbicacion AS CiudadUbicacion
                        ,plata.CodigoEstadoUbicacion AS CodigoEstadoUbicacion 
                        ,plata.CodigoPostalUbicacion AS CodigoPostalUbicacion 
                        ,plata.CodigoPais AS CodigoPaisTelefono
                        ,current_timestamp() AS FechaRegistro 
                        FROM 
                        (
                        SELECT 
                            A.Bk_IdCliente
                            ,B.nombre
                            ,B.apellido
                            ,D.CodigoPais
                            ,B.CalleUbicacion
                            ,B.CiudadUbicacion
                            ,B.CodigoEstadoUbicacion
                            ,B.CodigoPostalUbicacion
                        FROM 
                            {catalogoDataVault}.{esquemaDataVaultReg}.Hub_Clientes AS A 
                            INNER JOIN 
                            (
                            SELECT 
                                B2.hk_clientes
                                ,B2.nombre 
                                ,B2.apellido
                                ,B2.CalleUbicacion
                                ,B2.CiudadUbicacion
                                ,B2.CodigoEstadoUbicacion
                                ,B2.CodigoPostalUbicacion
                            FROM 
                                (
                                SELECT 
                                    hk_clientes
                                    ,nombre 
                                    ,apellido
                                    ,CalleUbicacion
                                    ,CiudadUbicacion
                                    ,CodigoEstadoUbicacion
                                    ,CodigoPostalUbicacion
                                    ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY FechaRegistro DESC) AS desduplicador
                                FROM 
                                    {catalogoDataVault}.{esquemaDataVaultReg}.Sat_Clientes_General 
                                ) AS B2
                            WHERE 
                                B2.desduplicador = 1
                            ) AS B 
                            ON A.hk_clientes = B.hk_clientes
                            INNER JOIN 
                            (
                            SELECT 
                                D2.hk_clientes
                                ,D2.CodigoPais
                            FROM 
                                (
                                SELECT 
                                hk_clientes
                                ,CASE 
                                    WHEN CodigoPais = '' THEN '(N/D)'
                                    ELSE CodigoPais
                                END CodigoPais
                                ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY FechaRegistro DESC) AS desduplicador
                                FROM 
                                {catalogoDataVault}.{esquemaDataVaultReg}.Sat_Clientes_Telefonos 
                                ) AS D2 
                            WHERE 
                                D2.desduplicador = 1
                            ) AS D 
                            ON A.hk_clientes = D.hk_clientes
                        ) AS plata 
                        LEFT JOIN {catalogoDwh}.{esquemaDwhReg}.Dim_Cliente AS dim 
                            ON plata.Bk_IdCliente = dim.LlaveNegocioIdCliente
                        WHERE 
                        dim.LlaveNegocioIdCliente IS NULL
                        ;
                         """)

dfDatosNuevosExistentes = dfDatosNuevosExistentes.unionAll(dfTempNuevos)

# Escribimos tanto los datos nuevos como los existentes con cambios usando pyspark con la función merge que es recomendada para estos casos

if dfDatosNuevosExistentes.isEmpty() == False : 
    dfDimCliente = DeltaTable.forName(spark, f"{catalogoDwh}.{esquemaDwhReg}.Dim_Cliente")
    dfDimCliente.alias("dim").merge(
        dfDatosNuevosExistentes.alias("plata"),
        "dim.LlaveNegocioIdCliente = plata.LlaveNegocioIdCliente"
        ,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


