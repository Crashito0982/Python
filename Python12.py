USE AT_CMDTS;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- Creamos o alteramos el Stored Procedure de PRUEBA
CREATE OR ALTER PROCEDURE [dbo].[CRM_LEGIT_FIRMAS_PRUEBA_001]
AS
BEGIN
    -- ==========================================================================================
    -- FECHA: 17-10-2025
    -- DESCRIPCIÓN: SP de prueba para extraer Contratos y Cédulas más recientes desde Axentria,
    --              replicando la lógica de negocio de "Entregado pero no Activado".
    -- ==========================================================================================

    SET NOCOUNT ON;

    -- Declaramos las variables a utilizar
    DECLARE @DIAS_ATRAS_EVENTOS numeric(3) = -30; -- Rango para buscar eventos de entrega
    DECLARE @FechaEventosDesde DATE = CONVERT(DATE, DATEADD(day, @DIAS_ATRAS_EVENTOS, GETDATE()));

    -- Limpieza de tablas temporales si existen
    DROP TABLE IF EXISTS #docids_procesados;
    DROP TABLE IF EXISTS #docids_pendientes;
    DROP TABLE IF EXISTS #contratos_a_activar;
    DROP TABLE IF EXISTS #cedulas_clientes_contrato;
    DROP TABLE IF EXISTS #cedulas_max_fecha;
    DROP TABLE IF EXISTS #cedulas_finales;
    DROP TABLE IF EXISTS [dbo].[crm_legit_axnt_base_modelo_PRUEBA];

    -- PASO 1: Identificar DOCIDs que ya completaron su ciclo (Activados, Destruidos, etc.)
    SELECT DISTINCT DOCID
    INTO #docids_procesados
    FROM [ODSP_LGLIB_LGMEVT]
    WHERE EVTTDI IN (16, 19, 70); -- 16: activacion, 19: destruido, 70: firma varia

    -- PASO 2: Identificar DOCIDs pendientes (Entregados pero no procesados)
    SELECT DISTINCT evt.docid, doc.doccti AS nro_cliente
    INTO #docids_pendientes
    FROM [ODSP_LGLIB_LGMEVT] evt
    INNER JOIN [ODSP_LGLIB_LGMDOC] doc ON evt.docid = doc.docid
    WHERE 
        evt.evttdi = 1 -- Evento: Entregado
        AND evt.evtfch >= @FechaEventosDesde
        AND evt.docid NOT IN (SELECT docid FROM #docids_procesados);
    
    -- PASO 3: Obtener los Contratos de Axentria para los DOCIDs pendientes
    SELECT
        CAST(CONVERT(NVARCHAR(4), e.ve_fechaCreacion, 112) AS INT) AS anho,
        CAST(CONVERT(NVARCHAR(6), e.ve_fechaCreacion, 112) AS INT) AS anho_mes,
        CAST(CONVERT(NVARCHAR(8), e.ve_fechaCreacion, 112) AS INT) AS anho_mes_dia,
        e.ve_fechaCreacion AS fecha_creacion,
        a.vcn_iddo,
        e.VE_ID,
        CONCAT(RIGHT(f.alfs_camino, 2), '\', a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
        d.co_nombre AS nombre_logico,
        c.td_nombre AS tipo_documento,
        'SI' AS renombrar_archivo,
        NULL AS nro_guia,
        FLOOR(a.vcn_valor) AS nro_cliente
    INTO #contratos_a_activar
    FROM [ODSP_AXNT_VALORCAMPONUM] a
    LEFT JOIN [ODSP_axnt_version] e ON a.vcn_iddo = e.ve_iddo
    LEFT JOIN [ODSP_axnt_tipodoc] c ON a.vcn_idtd = c.td_id
    LEFT JOIN [ODSP_axnt_contenido] d ON a.vcn_iddo = d.co_id
    LEFT JOIN [ODSP_axnt_AlmacenFS] f ON e.ve_idalmacen = f.alfs_id
    WHERE 
        c.td_id IN (134) -- TD_ID 134: CONTRATO DE TARJETA DE CRÉDITO
        AND a.vcn_iddo IN (SELECT docid FROM #docids_pendientes);

    -- PASO 4: Obtener la cédula más reciente para los clientes encontrados
    
    -- 4.1: Traer todas las cédulas de los clientes desde la tabla pre-calculada
    SELECT
        base.*
    INTO #cedulas_clientes_contrato
    FROM [dbo].[LD_CD_ANXT_2020_2025] base
    WHERE 
        base.valor IN (SELECT nro_cliente FROM #docids_pendientes) -- Usamos la lista de clientes pendientes
        AND base.alfs_camino IS NOT NULL
        AND base.nombre_fisico IS NOT NULL;

    -- 4.2: Calcular la fecha máxima de la cédula por cliente
    SELECT
        valor AS nro_cliente,
        MAX(fecha_creacion) AS max_fecha_creacion
    INTO #cedulas_max_fecha
    FROM #cedulas_clientes_contrato
    GROUP BY valor;

    -- 4.3: Filtrar solo la cédula que coincida con la fecha máxima
    SELECT
        c.anho,
        c.anho_mes,
        c.anho_mes_dia,
        c.fecha_creacion,
        c.vcn_iddo,
        c.VE_ID,
        c.nombre_fisico,
        c.nombre_logico,
        c.tipo_documento,
        c.renombrar_archivo,
        NULL AS nro_guia,
        c.valor AS nro_cliente
    INTO #cedulas_finales
    FROM #cedulas_clientes_contrato c
    INNER JOIN #cedulas_max_fecha m 
        ON c.valor = m.nro_cliente 
        AND c.fecha_creacion = m.max_fecha_creacion;

    -- PASO 5: Unir resultados e insertar en la tabla final de prueba
    CREATE TABLE [dbo].[crm_legit_axnt_base_modelo_PRUEBA] (
        [anho] [int] NULL,
        [anho_mes] [int] NULL,
        [anho_mes_dia] [int] NULL,
        [fecha_creacion] [datetime] NULL,
        [vcn_iddo] [numeric](10, 0) NULL,
        [VE_ID] [numeric](10, 0) NULL,
        [nombre_fisico] [varchar](255) NULL,
        [nombre_logico] [varchar](255) NULL,
        [tipo_documento] [varchar](255) NULL,
        [renombrar_archivo] [varchar](2) NULL,
        [nro_guia] [numeric](10, 0) NULL,
        [nro_cliente] [numeric](10, 0) NULL
    );

    -- Insertamos los contratos
    INSERT INTO [dbo].[crm_legit_axnt_base_modelo_PRUEBA]
    SELECT * FROM #contratos_a_activar;

    -- Insertamos las cédulas más recientes
    INSERT INTO [dbo].[crm_legit_axnt_base_modelo_PRUEBA]
    SELECT * FROM #cedulas_finales;

    -- Limpieza final de tablas temporales
    DROP TABLE IF EXISTS #docids_procesados;
    DROP TABLE IF EXISTS #docids_pendientes;
    DROP TABLE IF EXISTS #contratos_a_activar;
    DROP TABLE IF EXISTS #cedulas_clientes_contrato;
    DROP TABLE IF EXISTS #cedulas_max_fecha;
    DROP TABLE IF EXISTS #cedulas_finales;

    SET NOCOUNT OFF;
END;
GO