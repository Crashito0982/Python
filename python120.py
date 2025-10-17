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
    -- DESCRIPCIÓN: SP de prueba para extraer Contratos y Cédulas más recientes desde Axentria.
    --              Esta versión replica fielmente la lógica de negocio del SP de producción
    --              original para asegurar la correcta selección de documentos.
    -- ==========================================================================================

    SET NOCOUNT ON;

    -- Declaramos las variables a utilizar, replicando la nomenclatura original
    DECLARE @DIAS_ATRAS numeric(3) = -5; -- Rango de filtro final según nuevo requerimiento
    DECLARE @DIAS_ATRAS_EVENTOS_OUT numeric(3) = -30; -- Rango amplio para buscar eventos candidatos
    DECLARE @AMD_inicio varchar(8) = CONVERT(varchar, DATEADD(day, @DIAS_ATRAS, GETDATE()), 112);
    DECLARE @AMD_EVENTOS_OUT varchar(8) = CONVERT(varchar, DATEADD(day, @DIAS_ATRAS_EVENTOS_OUT, GETDATE()), 112);

    -- Limpieza de tablas temporales si existen
    DROP TABLE IF EXISTS #LS_EVENTO_ENTREGADO_TMP;
    DROP TABLE IF EXISTS #ag_eventos_entregados_base_temp;
    DROP TABLE IF EXISTS #ag_eventos_entregados_temp;
    DROP TABLE IF EXISTS #contratos_a_activar;
    DROP TABLE IF EXISTS #cedulas_clientes_contrato;
    DROP TABLE IF EXISTS #cedulas_max_fecha;
    DROP TABLE IF EXISTS #cedulas_finales;
    DROP TABLE IF EXISTS [dbo].[crm_legit_axnt_base_modelo_PRUEBA];

    -- PASO 1: (Réplica de @SQL_0) - Obtener el último evento de entrega de DOCIDs sin activar/destruir.
    SELECT a.docid, MAX(a.evtsc) AS evtsc
    INTO #LS_EVENTO_ENTREGADO_TMP
    FROM [ODSP_LGLIB_LGMEVT] a
    WHERE a.evttdi = 1 -- Evento: Entregado
      AND a.evtfch >= @AMD_EVENTOS_OUT
      AND a.docid NOT IN (SELECT DISTINCT DOCID FROM [ODSP_LGLIB_LGMEVT] WHERE EVTTDI IN (16, 19, 70)) -- Excluye activados, destruidos, etc.
    GROUP BY a.docid;

    -- PASO 2: (Réplica de @SQL_1) - Obtener los detalles completos del evento, aplicando el filtro de fecha estricto.
    SELECT a.*
    INTO #ag_eventos_entregados_base_temp
    FROM [ODSP_LGLIB_LGMEVT] a
    INNER JOIN #LS_EVENTO_ENTREGADO_TMP b ON b.docid = a.docid AND b.evtsc = a.evtsc
    WHERE a.evtfch >= @AMD_inicio
      AND a.evttdi = 1;

    -- PASO 3: (Réplica de @SQL_2) - Unir con la tabla maestra de documentos y aplicar filtros de negocio y fecha.
    SELECT a.DOCID, CAST(a.DOCCTI AS INT) AS nro_cliente
    INTO #ag_eventos_entregados_temp
    FROM [ODSP_LGLIB_LGMDOC] a
    LEFT JOIN [ODSP_LGLIB_LGMTDC] b ON a.DOCPRI = b.tdcpri AND a.DOCTDI = b.TDCID
    INNER JOIN #ag_eventos_entregados_base_temp c ON c.DOCID = a.DOCID
    WHERE a.docedf >= @AMD_inicio -- Segundo filtro de fecha crucial
      AND (
          (b.TDCPRI IN ('TARJ.C') AND b.TDCID IN (1, 36, 37, 70, 89, 116, 132))
          OR (b.TDCPRI = '105' AND b.TDCID = 89)
      );

    -- PASO 4: Obtener los Contratos de Axentria para los DOCIDs que pasaron todos los filtros.
    SELECT
        CAST(CONVERT(NVARCHAR(4), e.ve_fechaCreacion, 112) AS INT) AS anho,
        CAST(CONVERT(NVARCHAR(6), e.ve_fechaCreacion, 112) AS INT) AS anho_mes,
        CAST(CONVERT(NVARCHAR(8), e.ve_fechaCreacion, 112) AS INT) AS anho_mes_dia,
        e.ve_fechaCreacion AS fecha_creacion,
        a.vcn_iddo, e.VE_ID,
        CONCAT(RIGHT(f.alfs_camino, 2), '\', a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
        d.co_nombre AS nombre_logico, c.td_nombre AS tipo_documento,
        'SI' AS renombrar_archivo, NULL AS nro_guia, FLOOR(a.vcn_valor) AS nro_cliente
    INTO #contratos_a_activar
    FROM [ODSP_AXNT_VALORCAMPONUM] a
    INNER JOIN [ODSP_axnt_version] e ON a.vcn_iddo = e.ve_iddo
    INNER JOIN [ODSP_axnt_tipodoc] c ON a.vcn_idtd = c.td_id
    INNER JOIN [ODSP_axnt_contenido] d ON a.vcn_iddo = d.co_id
    INNER JOIN [ODSP_axnt_AlmacenFS] f ON e.ve_idalmacen = f.alfs_id
    WHERE c.td_id IN (134) -- TD_ID 134: CONTRATO DE TARJETA DE CRÉDITO
      AND a.vcn_iddo IN (SELECT docid FROM #ag_eventos_entregados_temp);

    -- PASO 5: Obtener la cédula más reciente para los clientes de los CONTRATOS encontrados.
    SELECT base.*
    INTO #cedulas_clientes_contrato
    FROM [dbo].[LD_CD_ANXT_2020_2025] base
    WHERE base.valor IN (SELECT nro_cliente FROM #contratos_a_activar)
      AND base.alfs_camino IS NOT NULL AND base.nombre_fisico IS NOT NULL;

    SELECT valor AS nro_cliente, MAX(fecha_creacion) AS max_fecha_creacion
    INTO #cedulas_max_fecha
    FROM #cedulas_clientes_contrato
    GROUP BY valor;

    SELECT c.anho, c.anho_mes, c.anho_mes_dia, c.fecha_creacion, c.vcn_iddo, c.VE_ID,
           c.nombre_fisico, c.nombre_logico, c.tipo_documento, c.renombrar_archivo,
           NULL AS nro_guia, c.valor AS nro_cliente
    INTO #cedulas_finales
    FROM #cedulas_clientes_contrato c
    INNER JOIN #cedulas_max_fecha m ON c.valor = m.nro_cliente AND c.fecha_creacion = m.max_fecha_creacion;

    -- PASO 6: Unir resultados e insertar en la tabla final de prueba.
    CREATE TABLE [dbo].[crm_legit_axnt_base_modelo_PRUEBA] (
        [anho] [int] NULL, [anho_mes] [int] NULL, [anho_mes_dia] [int] NULL,
        [fecha_creacion] [datetime] NULL, [vcn_iddo] [numeric](10, 0) NULL,
        [VE_ID] [numeric](10, 0) NULL, [nombre_fisico] [varchar](255) NULL,
        [nombre_logico] [varchar](255) NULL, [tipo_documento] [varchar](255) NULL,
        [renombrar_archivo] [varchar](2) NULL, [nro_guia] [numeric](10, 0) NULL,
        [nro_cliente] [numeric](10, 0) NULL
    );

    INSERT INTO [dbo].[crm_legit_axnt_base_modelo_PRUEBA] SELECT * FROM #contratos_a_activar;
    INSERT INTO [dbo].[crm_legit_axnt_base_modelo_PRUEBA] SELECT * FROM #cedulas_finales;
    
    -- PASO 7: Eliminar duplicados en la tabla final.
    ;WITH CTE AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY vcn_iddo, fecha_creacion ORDER BY (SELECT NULL)) AS duplicados
        FROM [dbo].[crm_legit_axnt_base_modelo_PRUEBA]
    )
    DELETE FROM CTE WHERE duplicados > 1;

    -- Limpieza final.
    DROP TABLE IF EXISTS #LS_EVENTO_ENTREGADO_TMP, #ag_eventos_entregados_base_temp, #ag_eventos_entregados_temp,
                         #contratos_a_activar, #cedulas_clientes_contrato, #cedulas_max_fecha, #cedulas_finales;

    SET NOCOUNT OFF;
END;
GO

