USE [MDWH_RAW]
GO
/****** Object:  StoredProcedure [dbo].[pf_create_columnstore_indexes_for_trans]    Script Date: 11.08.2022 9:54:33 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



create or ALTER    procedure [dbo].[pf_create_columnstore_indexes_for_trans]
         @table sysname 
		,@ps_name sysname 
		as 

begin try
/*
создает колумнстор индексы на доп.таблице для переливки данных в архивную фг
*/        

SET NOCOUNT ON


DECLARE @tableid int
DECLARE @schemaName sysname
DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @table_tmp sysname
DECLARE @msg NVARCHAR(max);

SELECT
	@table_tmp = @table + '_TMP_TRANS'


SELECT
	@tableid = OBJECT_ID(@table)
SELECT
	@schemaName = OBJECT_SCHEMA_NAME(@tableid)



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


SELECT

case when si.type  = 5 then 'CREATE CLUSTERED COLUMNSTORE INDEX ' +  QUOTENAME(si.name) + ' on ' +  @table_tmp 
	when si.type  =	 6 then 'CREATE NONCLUSTERED COLUMNSTORE INDEX ' +  QUOTENAME(si.name) + ' on ' +  @table_tmp  
	else ''
	end + 

        /* includes */ 
CASE WHEN include_definition IS NOT NULL and si.type = 6 THEN 
            N'(' + include_definition + N')'
            ELSE N''
        END +


        /* filters */ 
CASE WHEN filter_definition IS NOT NULL THEN 
            N' WHERE ' + filter_definition ELSE N''
        END +
        /* with clause - compression goes here */


CASE WHEN columnstore_compression_partition_list IS NOT NULL OR columnstore_archive_compression_partition_list IS NOT NULL 
        THEN N' WITH (' +
CASE WHEN columnstore_compression_partition_list IS NOT NULL 
		THEN N'DATA_COMPRESSION = COLUMNSTORE ' + 
CASE WHEN psc.name IS NULL THEN N'' 
	ELSE + N' '   --ON PARTITIONS (' + columnstore_compression_partition_list + N')
	END
ELSE N'' 
END +
CASE WHEN columnstore_compression_partition_list IS NOT NULL AND columnstore_archive_compression_partition_list IS NOT NULL 
		THEN N', ' 
	ELSE N'' 
END +
CASE WHEN columnstore_archive_compression_partition_list IS NOT NULL 
		THEN N'DATA_COMPRESSION = COLUMNSTORE_ARCHIVE ' + 
CASE WHEN psc.name IS NULL 
		THEN N'' 
	ELSE + N' ' --ON PARTITIONS (' + columnstore_archive_compression_partition_list + N')
END
ELSE N'' 
END
---comp columnstore
				+
				 CASE
					WHEN si.fill_factor = 0 THEN ' '
					ELSE +
						',FILLFACTOR = ' + CAST(si.fill_factor AS NVARCHAR(5))
				END +


--',
--ALLOW_ROW_LOCKS = ' + CASE
--							WHEN  si.allow_row_locks = 1 THEN ' ON'
--							ELSE +' OFF'
--						END + ',

--ALLOW_PAGE_LOCKS = ' + CASE
--							WHEN si.allow_page_locks = 1 THEN ' ON'
--							ELSE +' OFF'
--						END 
			


            + N')'
            ELSE N''
        END +
        /* ON where? filegroup? partition scheme? */
        ' ON ' + @ps_name -- CASE WHEN psc.name is null THEN ISNULL(QUOTENAME(fg.name),N'') ELSE @ps_name + N' (' + partitioning_column.column_name + N')' END
        + N';'
     AS index_create_statement
		, 0 as worked
into #create_ind		

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
    (
	SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
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
/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
        and p.data_compression = 3
    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS columnstore_compression_clause ( columnstore_compression_partition_list )
/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
        and p.data_compression = 4
    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS columnstore_archive_compression_clause ( columnstore_archive_compression_partition_list )


WHERE 
    si.type IN (5,6) 
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
		VALUES (@log_id, 'finished: ' + @sql, OBJECT_NAME(@@procid))
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
	VALUES (@log_id, 'закончили', OBJECT_NAME(@@procid))
-----
--log
-----


END TRY
BEGIN CATCH

	SELECT
		@msg = ERROR_MESSAGE()
	RAISERROR (@msg, 20, 1) WITH LOG

END CATCH
