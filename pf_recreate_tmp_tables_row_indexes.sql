USE [MDWH_RAW]
GO
/****** Object:  StoredProcedure [dbo].[pf_recreate_tmp_tables_row_indexes]    Script Date: 11.08.2022 9:56:10 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create or ALTER    PROCEDURE [dbo].[pf_recreate_tmp_tables_row_indexes]
		 @table sysname 
		,@ps_name sysname 
as

BEGIN TRY
/*
ребилдит нужную темповую таблицу на архивную фг, работает в случае, если в таблице нет lob'ов
*/
        
DECLARE @tableid int
DECLARE @schemaName sysname
DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @msg NVARCHAR(max);



SELECT
	@tableid = OBJECT_ID(@table)


DROP TABLE IF EXISTS #create_ind

-----
--log
-----
DECLARE @log_id INT
SELECT @log_id = MAX(lh.log_id) FROM log_headlog lh	
-----
--log
-----


-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'начали. параметры: ' + @table  + ', ' + @ps_name  , OBJECT_NAME(@@procid))
-----
--log
-----

/*
drop table #create_ind
*/





SELECT
	 
    CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */'
    ELSE 
        CASE is_primary_key WHEN 1 THEN

            N'CREATE UNIQUE ' +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
                N'INDEX ' + QUOTENAME(si.name) + N' ON ' + @table + N' '
                

            --N'ALTER TABLE ' + @table + N' ADD CONSTRAINT ' + QUOTENAME(si.name) + N' PRIMARY KEY ' +
            --    CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '

            ELSE N'CREATE ' + 
                CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
                N'INDEX ' + QUOTENAME(si.name) + N' ON ' + @table + N' '
        END +
        /* key def */ N'(' + key_definition + N')' +
        /* includes */ CASE WHEN include_definition IS NOT NULL THEN 
            N' INCLUDE (' + include_definition + N')'
            ELSE N''
        END +
        /* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
            N' WHERE ' + filter_definition ELSE N''
        END +		 N' WITH ( DROP_EXISTING = ON ' +
        /* with clause - compression goes here */
        CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
            THEN N' ,  ' +
                CASE WHEN row_compression_partition_list IS NOT NULL THEN
                    N'DATA_COMPRESSION = ROW ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ' END	 --ON PARTITIONS (' + row_compression_partition_list + N')
                ELSE N'' END +
                CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN N', ' ELSE N'' END +
                CASE WHEN page_compression_partition_list IS NOT NULL THEN
                    N'DATA_COMPRESSION = PAGE ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ' END --ON PARTITIONS (' + page_compression_partition_list + N')
                ELSE N'' END


				+
				 CASE
					WHEN si.fill_factor = 0 THEN ' '
					ELSE +
						',FILLFACTOR = ' + CAST(si.fill_factor AS NVARCHAR(5))
				END +


',ALLOW_ROW_LOCKS = ' + CASE
							WHEN si.allow_row_locks = 1 THEN ' ON'
							ELSE +' OFF'
						END + ', ALLOW_PAGE_LOCKS = ' + CASE
							WHEN si.allow_page_locks = 1 THEN ' ON'
							ELSE +' OFF'
						END 
			


            + N')'
            ELSE N')'
        END +
        /* ON where? filegroup? partition scheme? */
        ' ON [' + @ps_name  + N'];'
    END AS index_create_statement
		
	,0 AS worked

INTO #create_ind

FROM sys.indexes AS si
JOIN sys.tables AS t ON si.object_id=t.object_id
JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id
LEFT JOIN sys.dm_db_index_usage_stats AS stat ON 
    stat.database_id = DB_ID() 
    and si.object_id=stat.object_id 
    and si.index_id=stat.index_id
LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id
LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id
LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id
/* Key list */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + QUOTENAME(c.name) +
        CASE ic.is_descending_key WHEN 1 then N' DESC' ELSE N'' END
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = si.object_id
        and ic.index_id=si.index_id
        and ic.key_ordinal > 0
    ORDER BY ic.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS keys ( key_definition )
/* Partitioning Ordinal */ OUTER APPLY (
    SELECT MAX(QUOTENAME(c.name)) AS column_name
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = si.object_id
        and ic.index_id=si.index_id
        and ic.partition_ordinal = 1) AS partitioning_column
/* Include list */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + QUOTENAME(c.name)
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = si.object_id
        and ic.index_id=si.index_id
        and ic.is_included_column = 1
    ORDER BY c.name FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS includes ( include_definition )
/* Partitions */ OUTER APPLY ( 
    SELECT 
        COUNT(*) AS partition_count,
        CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_in_row_GB,
        CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_LOB_GB,
        SUM(ps.row_count) AS row_count
    FROM sys.partitions AS p
    JOIN sys.dm_db_partition_stats AS ps ON
        p.partition_id=ps.partition_id
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
    ) AS partition_sums
/* row compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
        and p.data_compression = 1
    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS row_compression_clause ( row_compression_partition_list )
/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
        and p.data_compression = 2
    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS page_compression_clause ( page_compression_partition_list )
WHERE 
    si.type IN (1,2) /* heap, clustered, nonclustered */
	and sc.name + '.' + t.name = @table
	order by si.index_id






WHILE EXISTS
(
	SELECT
		*
	FROM #create_ind
	WHERE worked = 0
)
BEGIN

		SELECT TOP 1
			@sql = index_create_statement
		FROM #create_ind
		WHERE worked = 0

		print @sql

		EXEC (@sql)

		-----
		--log
		-----
		INSERT INTO log_sublog (log_id, comment, procname)
			VALUES (@log_id, 'finished: ' + @sql , OBJECT_NAME(@@procid))
		-----
		--log
		-----



		UPDATE #create_ind
			SET worked = 1
		WHERE index_create_statement = @sql
END


-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'конец', OBJECT_NAME(@@procid))
-----
--log
-----


END TRY
BEGIN CATCH

SELECT
	@msg = ERROR_MESSAGE()

-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, @msg, OBJECT_NAME(@@procid))
-----
--log
-----

RAISERROR (@msg, 20, 1) WITH LOG
END CATCH
