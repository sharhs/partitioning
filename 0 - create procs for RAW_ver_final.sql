USE [MDWH_RAW]
GO

/****** Object:  StoredProcedure [dbo].[pf_create_columnstore_indexes_for_trans]    Script Date: 21.06.2022 15:37:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE  or alter procedure [dbo].[pf_create_columnstore_indexes_for_trans]
         @table sysname 
		,@ps_name sysname 
		as 

begin try
        

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
GO

/****** Object:  StoredProcedure [dbo].[pf_create_tables_indexes_for_trans]    Script Date: 21.06.2022 15:37:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE or alter PROCEDURE [dbo].[pf_create_tables_indexes_for_trans]
		 @table sysname 
		,@ps_name sysname 
as

BEGIN TRY

set NOCOUNT ON
        
DECLARE @tableid int
DECLARE @schemaName sysname
DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @table_tmp sysname
DECLARE @msg NVARCHAR(max);

SELECT
	@table_tmp = @table +'_TMP_TRANS'


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

/*
drop table #create_ind
*/


SET @sql = ';WITH index_column AS 
(SELECT ic.[object_id], ic.index_id, ic.is_descending_key, ic.is_included_column, c.name
FROM sys.index_columns ic 
JOIN sys.columns c ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
WHERE ic.[object_id] = ' + CAST(@tableid AS VARCHAR) + '
),
fk_columns AS (SELECT k.constraint_object_id, cname = c.name, rcname = rc.name
FROM sys.foreign_key_columns k  
JOIN sys.columns rc   ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id 
JOIN sys.columns c   ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
WHERE k.parent_object_id = ' + CAST(@tableid AS VARCHAR) + ')
SELECT @tblcr_tmp= ''CREATE TABLE '' + ''' + @table_tmp + ''' + ''('' + STUFF((SELECT CHAR(9) + '', ['' + c.name + ''] '' + CASE WHEN c.is_computed = 1 THEN ''AS '' + cc.[definition] + CASE WHEN cc.is_persisted = 1 THEN''PERSISTED'' ELSE '''' END ELSE UPPER(tp.name) +  CASE WHEN tp.name IN (''varchar'', ''char'', ''varbinary'', ''binary'', ''text'')
THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length AS VARCHAR(5)) END + '')'' WHEN tp.name IN (''nvarchar'', ''nchar'', ''ntext'') THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + '')'' WHEN tp.name IN (''datetime2'', ''time2'', ''datetimeoffset'') 
THEN ''('' + CAST(c.scale AS VARCHAR(5)) + '')'' WHEN tp.name IN( ''decimal'',''numeric'') THEN ''('' + CAST(c.[precision] AS VARCHAR(5)) + '','' + CAST(c.scale AS VARCHAR(5)) + '')'' ELSE '''' END +
CASE WHEN c.collation_name IS NOT NULL THEN '' COLLATE '' + c.collation_name ELSE '''' END + CASE WHEN c.is_nullable = 1 THEN '' NULL'' ELSE '' NOT NULL'' END + CASE WHEN dc.[definition] IS NOT NULL THEN '' DEFAULT'' + dc.[definition] ELSE '''' END + 
CASE WHEN ic.is_identity = 1 THEN '' IDENTITY('' + CAST(ISNULL(ic.seed_value, ''0'') AS CHAR(1)) + '','' + CAST(ISNULL(ic.increment_value, ''1'') AS CHAR(1)) + '')'' ELSE '''' END END  FROM sys.columns c JOIN sys.types tp   ON c.user_type_id = tp.user_type_id  LEFT JOIN sys.computed_columns cc   ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
LEFT JOIN sys.default_constraints dc   ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
LEFT JOIN sys.identity_columns ic   ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
WHERE c.[object_id] = ' + CAST(@tableid AS VARCHAR) + ' 
ORDER BY c.column_id FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, CHAR(9) + '' '')  + ISNULL((SELECT CHAR(9) + '', CONSTRAINT ['' + k.name + ''_n1] PRIMARY KEY '' 
+ CASE WHEN  i.type = 1 THEN +'' CLUSTERED ('' ELSE +'' NONCLUSTERED ('' END + (SELECT STUFF((  SELECT '', ['' + c.name + ''] '' + CASE WHEN ic.is_descending_key = 1 THEN ''DESC'' ELSE ''ASC'' END
FROM sys.index_columns ic JOIN sys.columns c  ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id left join sys.indexes i on ic.object_id=i.object_id and ic.index_id= i.index_id
WHERE ic.is_included_column = 0 AND ic.[object_id] = k.parent_object_id AND ic.index_id = k.unique_index_id
FOR XML PATH(N''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, ''''))
+ '')'' FROM sys.key_constraints k  left join sys.indexes i on i.object_id=k.parent_object_id and i.is_primary_key=1
WHERE k.parent_object_id = ' + CAST(@tableid AS VARCHAR) + ' AND k.[type] = ''PK''), '''') + '')   on ' + CAST(@ps_name AS VARCHAR) + ''''

set @sql = REPLACE(@sql,'_n1', '_n1_TRANS')

EXEC sp_executesql @sql
				  ,N'@tblcr_tmp varchar(max) out'
				  ,@tblcr OUT


PRINT @tblcr

EXEC (@tblcr)


-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'создали темповую таблицу ' + @table_tmp, OBJECT_NAME(@@procid))
-----
--log
-----



SELECT
	 
    CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */'
    ELSE 
        CASE is_primary_key WHEN 1 THEN
            N'ALTER TABLE ' + @table_tmp + N' ADD CONSTRAINT ' + QUOTENAME(si.name) + N' PRIMARY KEY ' +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '
            ELSE N'CREATE ' + 
                CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
                N'INDEX ' + QUOTENAME(si.name) + N' ON ' + @table_tmp + N' '
        END +
        /* key def */ N'(' + key_definition + N')' +
        /* includes */ CASE WHEN include_definition IS NOT NULL THEN 
            N' INCLUDE (' + include_definition + N')'
            ELSE N''
        END +
        /* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
            N' WHERE ' + filter_definition ELSE N''
        END +
        /* with clause - compression goes here */
        CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
            THEN N' WITH (' +
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
            ELSE N''
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
	and is_primary_key <>1
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
GO

/****** Object:  StoredProcedure [dbo].[pf_create_temp_columnstore_indexes]    Script Date: 21.06.2022 15:37:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE or alter  procedure [dbo].[pf_create_temp_columnstore_indexes]
         @table sysname 
		,@ps_name sysname 
		,@ps_colname sysname 
		,@p_num BIGINT
		as 

begin try
set NOCOUNT ON
        
DECLARE @tableid int
DECLARE @schemaName sysname
DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @table_tmp sysname
DECLARE @msg NVARCHAR(max);

SELECT
	@table_tmp = @table + '_' + CAST(@p_num AS NVARCHAR(99)) + '_TMP'


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
	VALUES (@log_id, 'начали. параметры: ' + @table  + ', ' + @ps_name + ', ' + @ps_colname , OBJECT_NAME(@@procid))
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
GO

/****** Object:  StoredProcedure [dbo].[pf_create_tmp_tables_indexes]    Script Date: 21.06.2022 15:37:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE or alter  PROCEDURE [dbo].[pf_create_tmp_tables_indexes]
		 @table sysname 
		,@ps_name sysname 
		,@ps_colname sysname 
		,@p_num bigint
as

BEGIN TRY

        
DECLARE @tableid int
DECLARE @schemaName sysname
DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @table_tmp sysname
DECLARE @msg NVARCHAR(max);

SELECT
	@table_tmp = @table + '_' + CAST(@p_num AS NVARCHAR(99)) + '_TMP'


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
	VALUES (@log_id, 'начали. параметры: ' + @table  + ', ' + @ps_name + ', ' + @ps_colname , OBJECT_NAME(@@procid))
-----
--log
-----

/*
drop table #create_ind
*/


SET @sql = ';WITH index_column AS 
(SELECT ic.[object_id], ic.index_id, ic.is_descending_key, ic.is_included_column, c.name
FROM sys.index_columns ic 
JOIN sys.columns c ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
WHERE ic.[object_id] = ' + CAST(@tableid AS VARCHAR) + '
),
fk_columns AS (SELECT k.constraint_object_id, cname = c.name, rcname = rc.name
FROM sys.foreign_key_columns k  
JOIN sys.columns rc   ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id 
JOIN sys.columns c   ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
WHERE k.parent_object_id = ' + CAST(@tableid AS VARCHAR) + ')
SELECT @tblcr_tmp= ''CREATE TABLE '' + ''' + @table_tmp + ''' + ''('' + STUFF((SELECT CHAR(9) + '', ['' + c.name + ''] '' + CASE WHEN c.is_computed = 1 THEN ''AS '' + cc.[definition] + CASE WHEN cc.is_persisted = 1 THEN''PERSISTED'' ELSE '''' END ELSE UPPER(tp.name) +  CASE WHEN tp.name IN (''varchar'', ''char'', ''varbinary'', ''binary'', ''text'')
THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length AS VARCHAR(5)) END + '')'' WHEN tp.name IN (''nvarchar'', ''nchar'', ''ntext'') THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + '')'' WHEN tp.name IN (''datetime2'', ''time2'', ''datetimeoffset'') 
THEN ''('' + CAST(c.scale AS VARCHAR(5)) + '')'' WHEN tp.name IN( ''decimal'',''numeric'') THEN ''('' + CAST(c.[precision] AS VARCHAR(5)) + '','' + CAST(c.scale AS VARCHAR(5)) + '')'' ELSE '''' END +
CASE WHEN c.collation_name IS NOT NULL THEN '' COLLATE '' + c.collation_name ELSE '''' END + CASE WHEN c.is_nullable = 1 THEN '' NULL'' ELSE '' NOT NULL'' END + CASE WHEN dc.[definition] IS NOT NULL THEN '' DEFAULT'' + dc.[definition] ELSE '''' END + 
CASE WHEN ic.is_identity = 1 THEN '' IDENTITY('' + CAST(ISNULL(ic.seed_value, ''0'') AS CHAR(1)) + '','' + CAST(ISNULL(ic.increment_value, ''1'') AS CHAR(1)) + '')'' ELSE '''' END END  FROM sys.columns c JOIN sys.types tp   ON c.user_type_id = tp.user_type_id  LEFT JOIN sys.computed_columns cc   ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
LEFT JOIN sys.default_constraints dc   ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
LEFT JOIN sys.identity_columns ic   ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
WHERE c.[object_id] = ' + CAST(@tableid AS VARCHAR) + ' 
ORDER BY c.column_id FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, CHAR(9) + '' '')  + ISNULL((SELECT CHAR(9) + '', CONSTRAINT ['' + k.name + ''_n1] PRIMARY KEY '' 
+ CASE WHEN  i.type = 1 THEN +'' CLUSTERED ('' ELSE +'' NONCLUSTERED ('' END + (SELECT STUFF((  SELECT '', ['' + c.name + ''] '' + CASE WHEN ic.is_descending_key = 1 THEN ''DESC'' ELSE ''ASC'' END
FROM sys.index_columns ic JOIN sys.columns c  ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id left join sys.indexes i on ic.object_id=i.object_id and ic.index_id= i.index_id
WHERE ic.is_included_column = 0 AND ic.[object_id] = k.parent_object_id AND ic.index_id = k.unique_index_id
FOR XML PATH(N''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, ''''))
+ '')'' FROM sys.key_constraints k  left join sys.indexes i on i.object_id=k.parent_object_id and i.is_primary_key=1
WHERE k.parent_object_id = ' + CAST(@tableid AS VARCHAR) + ' AND k.[type] = ''PK''), '''') + '')   on ' + CAST(@ps_name AS VARCHAR) + ''''

set @sql = REPLACE(@sql,'_n1',cast(@p_num as nvarchar(9)) +'_n1')

EXEC sp_executesql @sql
				  ,N'@tblcr_tmp varchar(max) out'
				  ,@tblcr OUT


PRINT @tblcr

EXEC (@tblcr)


-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'создали темповую таблицу ' + @table_tmp, OBJECT_NAME(@@procid))
-----
--log
-----



SELECT
	 
    CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */'
    ELSE 
        CASE is_primary_key WHEN 1 THEN
            N'ALTER TABLE ' + @table_tmp + N' ADD CONSTRAINT ' + QUOTENAME(si.name) + N' PRIMARY KEY ' +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '
            ELSE N'CREATE ' + 
                CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
                N'INDEX ' + QUOTENAME(si.name) + N' ON ' + @table_tmp + N' '
        END +
        /* key def */ N'(' + key_definition + N')' +
        /* includes */ CASE WHEN include_definition IS NOT NULL THEN 
            N' INCLUDE (' + include_definition + N')'
            ELSE N''
        END +
        /* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
            N' WHERE ' + filter_definition ELSE N''
        END +
        /* with clause - compression goes here */
        CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
            THEN N' WITH (' +
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
            ELSE N''
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
	and is_primary_key <>1
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
GO

/****** Object:  StoredProcedure [dbo].[pf_merge_split_partitions]    Script Date: 21.06.2022 15:37:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE  or alter PROCEDURE [dbo].[pf_merge_split_partitions]
		 @pf	sysname 
		,@ps	sysname 
as 
BEGIN TRY

SET XACT_ABORT, NOCOUNT ON;

/*


select * from ##tmp_tables
order by 1,2,4

exec pf_merge_split_partitions
		 @pf	= 'pfunction_date'
		,@ps	= 'pscheme_date'
		

*/

	DECLARE @msg NVARCHAR(max);

	declare @tbl		sysname 
			,@tbl_TMP   sysname
			,@p_num		BIGINT
		



	declare @pt_number_cur		int
			,@pt_number_left	int
			,@pt_number_right	int
			,@getdate			datetime =  DATEADD(m, DATEDIFF(m, 0, GETDATE()), 0)  --		 '2022-12-01 00:00:00'	
			,@date_cur			datetime
			,@date_left			datetime
			,@date_right		datetime
			,@sql				nvarchar(max)


-----
--log
-----
DECLARE @log_id INT
SELECT @log_id = MAX(lh.log_id) FROM log_headlog lh	
-----
--log
-----




	select @date_cur = dateadd(month,-7,@getdate)

	set @date_left = dateadd(month,-1,@date_cur)
	set @date_right = dateadd(month,1,@date_cur)


--	print @getdate
--	print @date_cur


	select 
		@pt_number_cur	= partition_number
	from #tmp_tables
	where bound_value = @date_cur
	and pf_name = @pf


	set @pt_number_left	= @pt_number_cur - 1
	set @pt_number_right = @pt_number_cur + 1


-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'начали. параметры: ' + @pf + ', ' 
									       + @ps  + ', '
										   + cast(@pt_number_cur as nvarchar(12)) + ', ' 
										   + cast(@pt_number_left as nvarchar(12)) + ', ' 
										   + cast(@pt_number_right as nvarchar(12))
		, OBJECT_NAME(@@procid))
-----
--log
-----



	drop table if exists #tbls

	select distinct		
		table_name as tbl 
		,table_name + '_' + CAST(partition_number AS NVARCHAR(99))  + '_TMP' as tbl_tmp
		,partition_number
		,0 as worked
		,0 as returned
	into #tbls
	from #tmp_tables
	where pf_name = @pf
			
			
	 
	/*
	select 
	'#tmp_tables' tbl,
	* from #tbls





		   drop table #tbls
	select distinct		
		schema_name + '.' + object_name as tbl 
		,schema_name + '.' + object_name  + '_TMP' as tbl_tmp
		,0 as worked
	from ##part_info

	delete ##part_info
	where object_name like '%tmp'

	*/


	while exists(select * from #tbls where worked =0)
	begin

		select top 1
			@tbl = tbl
			,@tbl_TMP = tbl_TMP
			,@p_num   = partition_number
		from #tbls
		where worked =0
		order by tbl


		set @sql = 'alter table '+@tbl+' switch partition '+cast(@p_num as nvarchar(12))+' to '+@tbl_TMP+' with (wait_at_low_priority (max_duration = 1 minutes, abort_after_wait = blockers));'
		exec(@sql)
		print @sql


		update #tbls
		set worked =1
		where 
		tbl = @tbl 
		and tbl_TMP = @tbl_TMP 
		AND partition_number = @p_num

		-----
		--log
		-----
		INSERT INTO log_sublog (log_id, comment, procname)
			VALUES (@log_id, 'свичнули: ' + @sql, OBJECT_NAME(@@procid))
		-----
		--log
		-----
	
	end

-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'свичнули таблицы' , OBJECT_NAME(@@procid))
-----
--log
-----





	set @sql = 'ALTER PARTITION SCHEME '+@ps+' NEXT USED [MDWH_RAW_HISTORY]'
	exec(@sql)
	--print @sql

	set @sql = 'ALTER PARTITION FUNCTION '+@pf+'() MERGE RANGE ('''+cast(@date_cur as nvarchar(22))+''')'
	exec(@sql)
	--print @sql

-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'мерджнули: ' + @sql, OBJECT_NAME(@@procid))
-----
--log
-----


	set @sql = 'ALTER PARTITION SCHEME '+@ps+' NEXT USED [MDWH_RAW_HISTORY]'
	exec(@sql)
	--print @sql



	set @sql = 'ALTER PARTITION FUNCTION '+@pf+'() SPLIT RANGE ('''+cast(@date_cur as nvarchar(22))+''')'
	exec(@sql)
	--print @sql



-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'сплитанули: ' + @sql, OBJECT_NAME(@@procid))
-----
--log
-----



-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'перестраиваем темповые таблицы на MDWH_RAW_HISTORY', OBJECT_NAME(@@procid))
-----
--log
-----
/*
select * from #tbls
SELECT * FROM #tbls_to_recreate
*/
	DECLARE @tbl_i sysname
		,@ps_name_i SYSNAME	  = 'MDWH_RAW_HISTORY'
		,@tbl_i_trans sysname

	DROP TABLE IF EXISTS #tbls_to_recreate

	SELECT
		OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id)	table_name
	   ,i.name
	   ,i.type
	   ,i.type_desc
	   ,0 AS worked
	   INTO #tbls_to_recreate
	FROM #tbls t
	JOIN sys.indexes i
		ON tbl_tmp = OBJECT_SCHEMA_NAME(i.object_id) + '.' + OBJECT_NAME(i.object_id) 
	WHERE i.type IN( 1,5)
	AND partition_number = @pt_number_cur

	SELECT * FROM #tbls_to_recreate




	WHILE EXISTS(SELECT * FROM #tbls_to_recreate WHERE worked = 0)
	BEGIN

		SELECT TOP 1 
			@tbl_i=  table_name
		FROM #tbls_to_recreate ttr
		WHERE worked = 0


		if EXISTS(
					select 
					object_schema_name(i.object_id) + '.' +object_name(i.object_id) tbl, 
					au.type_desc,
					sum(au.used_pages)used_pages  from 
					sys.indexes AS i WITH (NOLOCK)
					INNER JOIN sys.partitions AS p WITH (NOLOCK)
					ON i.[object_id] = p.[object_id]
					AND i.index_id = p.index_id
					INNER JOIN sys.allocation_units AS au WITH (NOLOCK)
					ON p.hobt_id = au.container_id
					where object_schema_name(i.object_id) + '.' +object_name(i.object_id)  = @tbl_i
					and au.type_desc = 'LOB_DATA'
					group by i.object_id,au.type_desc
				
				)
		BEGIN
			
			SET @tbl_i_trans = @tbl_i + '_TMP_TRANS'

			exec pf_create_tables_indexes_for_trans
					 @table = @tbl_i
					,@ps_name  ='MDWH_RAW_HISTORY'

			exec pf_create_columnstore_indexes_for_trans
					 @table = @tbl_i
					,@ps_name  ='MDWH_RAW_HISTORY'  	
		
			exec pf_trans_lobs_and_rename
					 @tbl_i		
				    ,@tbl_i_trans 

		END
		ELSE
		BEGIN

		
			EXEC pf_recreate_tmp_tables_row_indexes
			 @tbl_i
			,@ps_name_i			   
		
			EXEC pf_recreate_tmp_tables_columnstore_indexes 
					@tbl_i,
					@ps_name_i
		END 

		UPDATE #tbls_to_recreate 
		SET worked = 1
		WHERE table_name = @tbl_i
		

	END 

-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'перестроили ' , OBJECT_NAME(@@procid))
-----
--log
-----



-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'создаем констрейнты ' , OBJECT_NAME(@@procid))
-----
--log
-----




	DROP TABLE IF EXISTS #constraints
	DECLARE @ck SYSNAME
	,@bound_value SYSNAME
	,@bound_value_s SYSNAME 
	,@ps_colname SYSNAME 
	,@pn BIGINT 


	SELECT
		schema_name +'.' + object_name + '_' + CAST(partition_number AS NVARCHAR(9)) + '_TMP' AS tmp_table
		,'ck_' + object_name + '_' + CAST(partition_number AS NVARCHAR(9)) + '_TMP' AS tmp_CK
		,ps_colname
		,partition_number
		,CAST(CAST(bound_value AS NVARCHAR(30)) AS DATEtime) bound_value

		,0 AS worked 
   
		INTO #constraints

	FROM #part_info
	WHERE partition_number IN(@pt_number_cur,@pt_number_left ,@pt_number_right)
	AND ps_name	= @ps
	AND pf_name = @pf


		/*
	
	SELECT '#constraints' AS i
	 ,* FROM #constraints

		  */


	WHILE EXISTS(SELECT * FROM #constraints WHERE worked =0)
	BEGIN

		SELECT TOP 1
			@tbl_TMP = tmp_table
		   ,@ck = tmp_CK
		   ,@ps_colname = ps_colname
		   ,@pn = partition_number
		   ,@bound_value = bound_value 
		FROM   #constraints
		WHERE worked = 0

		SET @bound_value_s = DATEADD(MONTH,1,@bound_value)

		PRINT  CONVERT( NVARCHAR(30), @bound_value ,120)
		PRINT  CONVERT( NVARCHAR(30), @bound_value_s ,120)

		SET @sql = 'ALTER TABLE ' + @tbl_TMP + ' ADD CONSTRAINT ' + @ck + ' CHECK(' + @ps_colname + '>=''' + CONVERT( NVARCHAR(30), @bound_value ,120) + ''' AND ' + @ps_colname + '<''' + CONVERT( NVARCHAR(30), @bound_value_s ,120) + ''')'

		PRINT @sql

		EXEC (@sql)

		update   #constraints
		SET worked = 1
		WHERE   tmp_table = @tbl_TMP
			and tmp_CK = @ck 
			and ps_colname = @ps_colname 
			and partition_number = @pn 

	END







-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'создали ' , OBJECT_NAME(@@procid))
-----
--log
-----




			/*

	  SELECT * FROM #tbls
			  */
	while exists(select * from #tbls where returned =0)
	begin

		select top 1
			@tbl = tbl
			,@tbl_TMP = tbl_TMP
			,@p_num   = partition_number
		from #tbls
		where returned = 0
		order by tbl



		set @sql = 'alter table '+@tbl_TMP+' switch to '+@tbl+' partition '+cast(@p_num as nvarchar(12))+' with (wait_at_low_priority (max_duration = 1 minutes, abort_after_wait = blockers));'
		
		print @sql
		WAITFOR DELAY'00:00:01'
		
		exec(@sql)
		

-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'свичнули обратно в ' + @tbl, OBJECT_NAME(@@procid))
-----
--log
-----

		   update #tbls
		   set returned =1
		   where 
			tbl =@tbl 
			and tbl_TMP = @tbl_TMP 
			AND  partition_number = @p_num   
	
	end




-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'конец ' , OBJECT_NAME(@@procid))
-----
--log
-----


END TRY
BEGIN CATCH

	SELECT @msg = ERROR_MESSAGE()
	RAISERROR(@msg, 20, 1) with LOG
	/*
	  SELECT
		  ERROR_NUMBER() AS ErrorNumber
		 ,ERROR_SEVERITY() AS ErrorSeverity
		 ,ERROR_STATE() AS ErrorState
		 ,ERROR_PROCEDURE() AS ErrorProcedure
		 ,ERROR_LINE() AS ErrorLine
		 ,ERROR_MESSAGE() AS ErrorMessage
		 ,XACT_STATE() as XactState;
		 
		 */
END CATCH
GO

/****** Object:  StoredProcedure [dbo].[pf_recreate_tmp_tables_columnstore_indexes]    Script Date: 21.06.2022 15:37:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE or alter  PROCEDURE [dbo].[pf_recreate_tmp_tables_columnstore_indexes] 
				@table_tmp SYSNAME,
				@ps_TMP sysname
AS

BEGIN TRY

SET NOCOUNT ON


DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @msg NVARCHAR(max);

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
	VALUES (@log_id, 'начали. параметры: ' + @table_tmp  + ', ' + @ps_TMP  , OBJECT_NAME(@@procid))
-----
--log
-----

SELECT

	CASE
		WHEN si.type = 5 THEN 'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(si.name) + ' on ' + @table_tmp
		WHEN si.type = 6 THEN 'CREATE NONCLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(si.name) + ' on ' + @table_tmp
		ELSE ''
	END +

	/* includes */
	CASE
		WHEN include_definition IS NOT NULL AND
			si.type = 6 THEN N'(' + include_definition + N')'
		ELSE N''
	END +


	/* filters */
	CASE
		WHEN filter_definition IS NOT NULL THEN N' WHERE ' + filter_definition
		ELSE N''
	END +
	/* with clause - compression goes here */
		   N' WITH ( DROP_EXISTING = ON' +

	CASE
		WHEN columnstore_compression_partition_list IS NOT NULL OR
			columnstore_archive_compression_partition_list IS NOT NULL THEN N', ' +
			CASE
				WHEN columnstore_compression_partition_list IS NOT NULL THEN N'DATA_COMPRESSION = COLUMNSTORE ' +
					CASE
						WHEN psc.name IS NULL THEN N''
						ELSE +N' ON PARTITIONS (' + columnstore_compression_partition_list + N')'
					END
				ELSE N''
			END +
			CASE
				WHEN columnstore_compression_partition_list IS NOT NULL AND
					columnstore_archive_compression_partition_list IS NOT NULL THEN N', '
				ELSE N''
			END +
			CASE
				WHEN columnstore_archive_compression_partition_list IS NOT NULL THEN N'DATA_COMPRESSION = COLUMNSTORE_ARCHIVE ' +
					CASE
						WHEN psc.name IS NULL THEN N''
						ELSE +N' ON PARTITIONS (' + columnstore_archive_compression_partition_list + N')'
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



			+N')'
		ELSE +N')'
	END +
	/* ON where? filegroup? partition scheme? */
	' ON [' +  @ps_TMP + N'];'
	AS index_create_statement
   ,0 AS worked
INTO #create_ind

FROM sys.indexes AS si
JOIN sys.tables AS t
	ON si.object_id = t.object_id
JOIN sys.schemas AS sc
	ON t.schema_id = sc.schema_id
LEFT JOIN sys.dm_db_index_usage_stats AS stat
	ON stat.database_id = DB_ID()
		AND si.object_id = stat.object_id
		AND si.index_id = stat.index_id
LEFT JOIN sys.partition_schemes AS psc
	ON si.data_space_id = psc.data_space_id
LEFT JOIN sys.partition_functions AS pf
	ON psc.function_id = pf.function_id
LEFT JOIN sys.filegroups AS fg
	ON si.data_space_id = fg.data_space_id
/* Key list */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + QUOTENAME(c.name) +
				CASE ic.is_descending_key
					WHEN 1 THEN N' DESC'
					ELSE N''
				END
			FROM sys.index_columns AS ic
			JOIN sys.columns AS c
				ON ic.column_id = c.column_id
				AND ic.object_id = c.object_id
			WHERE ic.object_id = si.object_id
			AND ic.index_id = si.index_id
			AND ic.key_ordinal > 0
			ORDER BY ic.key_ordinal
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS keys (key_definition)
/* Partitioning Ordinal */ OUTER APPLY (SELECT
		MAX(QUOTENAME(c.name)) AS column_name
	FROM sys.index_columns AS ic
	JOIN sys.columns AS c
		ON ic.column_id = c.column_id
		AND ic.object_id = c.object_id
	WHERE ic.object_id = si.object_id
	AND ic.index_id = si.index_id
	AND ic.partition_ordinal = 1) AS partitioning_column
/* Include list */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + QUOTENAME(c.name)
			FROM sys.index_columns AS ic
			JOIN sys.columns AS c
				ON ic.column_id = c.column_id
				AND ic.object_id = c.object_id
			WHERE ic.object_id = si.object_id
			AND ic.index_id = si.index_id
			AND ic.is_included_column = 1
			ORDER BY c.name
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS includes (include_definition)
/* Partitions */ OUTER APPLY (SELECT
		COUNT(*) AS partition_count
	   ,CAST(SUM(ps.in_row_reserved_page_count) * 8. / 1024. / 1024. AS NUMERIC(32, 1)) AS reserved_in_row_GB
	   ,CAST(SUM(ps.lob_reserved_page_count) * 8. / 1024. / 1024. AS NUMERIC(32, 1)) AS reserved_LOB_GB
	   ,SUM(ps.row_count) AS row_count
	FROM sys.partitions AS p
	JOIN sys.dm_db_partition_stats AS ps
		ON p.partition_id = ps.partition_id
	WHERE p.object_id = si.object_id
	AND p.index_id = si.index_id) AS partition_sums
/* row compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 1
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS row_compression_clause (row_compression_partition_list)
/* data compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 2
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS page_compression_clause (page_compression_partition_list)
/* data compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 3
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS columnstore_compression_clause (columnstore_compression_partition_list)
/* data compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 4
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS columnstore_archive_compression_clause (columnstore_archive_compression_partition_list)


WHERE si.type IN (5,6)
AND sc.name + '.' + t.name = @table_tmp
ORDER BY si.index_id


WHILE EXISTS (SELECT * FROM #create_ind WHERE worked =0)
BEGIN
	SELECT TOP 1 @sql = ci.index_create_statement FROM #create_ind ci 
	WHERE ci.worked = 0

	EXEC(@sql)
	PRINT @sql

	UPDATE #create_ind
	SET worked=1
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
GO

/****** Object:  StoredProcedure [dbo].[pf_recreate_tmp_tables_row_indexes]    Script Date: 21.06.2022 15:37:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE or alter  PROCEDURE [dbo].[pf_recreate_tmp_tables_row_indexes]
		 @table sysname 
		,@ps_name sysname 
as

BEGIN TRY

        
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
GO

/****** Object:  StoredProcedure [dbo].[pf_script_columnstore_definition]    Script Date: 21.06.2022 15:37:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE  or alter PROCEDURE [dbo].[pf_script_columnstore_definition]
				@table_tmp SYSNAME,
				@ps_TMP sysname
AS

BEGIN TRY

SET NOCOUNT ON


DECLARE @sql nvarchar(max),@tblcr NVARCHAR(MAX) = ''
DECLARE @msg NVARCHAR(max);

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
	VALUES (@log_id, 'начали. параметры: ' + @table_tmp  + ', ' + @ps_TMP  , OBJECT_NAME(@@procid))
-----
--log
-----

SELECT

	CASE
		WHEN si.type = 5 THEN 'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(si.name) + ' on ' + @table_tmp
		WHEN si.type = 6 THEN 'CREATE NONCLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(si.name) + ' on ' + @table_tmp
		ELSE ''
	END +

	/* includes */
	CASE
		WHEN include_definition IS NOT NULL AND
			si.type = 6 THEN N'(' + include_definition + N')'
		ELSE N''
	END +


	/* filters */
	CASE
		WHEN filter_definition IS NOT NULL THEN N' WHERE ' + filter_definition
		ELSE N''
	END +
	/* with clause - compression goes here */
		   N' WITH ( DROP_EXISTING = ON' +

	CASE
		WHEN columnstore_compression_partition_list IS NOT NULL OR
			columnstore_archive_compression_partition_list IS NOT NULL THEN N', ' +
			CASE
				WHEN columnstore_compression_partition_list IS NOT NULL THEN N'DATA_COMPRESSION = COLUMNSTORE ' +
					CASE
						WHEN psc.name IS NULL THEN N''
						ELSE +N' ON PARTITIONS (' + columnstore_compression_partition_list + N')'
					END
				ELSE N''
			END +
			CASE
				WHEN columnstore_compression_partition_list IS NOT NULL AND
					columnstore_archive_compression_partition_list IS NOT NULL THEN N', '
				ELSE N''
			END +
			CASE
				WHEN columnstore_archive_compression_partition_list IS NOT NULL THEN N'DATA_COMPRESSION = COLUMNSTORE_ARCHIVE ' +
					CASE
						WHEN psc.name IS NULL THEN N''
						ELSE +N' ON PARTITIONS (' + columnstore_archive_compression_partition_list + N')'
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



			+N')'
		ELSE +N')'
	END +
	/* ON where? filegroup? partition scheme? */
	' ON ' +
	CASE
		WHEN psc.name IS NULL THEN ISNULL(QUOTENAME(fg.name), N'')
		ELSE @ps_TMP + N' (' + partitioning_column.column_name + N')'
	END
	+ N';'
	AS index_create_statement
   ,0 AS worked


FROM sys.indexes AS si
JOIN sys.tables AS t
	ON si.object_id = t.object_id
JOIN sys.schemas AS sc
	ON t.schema_id = sc.schema_id
LEFT JOIN sys.dm_db_index_usage_stats AS stat
	ON stat.database_id = DB_ID()
		AND si.object_id = stat.object_id
		AND si.index_id = stat.index_id
LEFT JOIN sys.partition_schemes AS psc
	ON si.data_space_id = psc.data_space_id
LEFT JOIN sys.partition_functions AS pf
	ON psc.function_id = pf.function_id
LEFT JOIN sys.filegroups AS fg
	ON si.data_space_id = fg.data_space_id
/* Key list */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + QUOTENAME(c.name) +
				CASE ic.is_descending_key
					WHEN 1 THEN N' DESC'
					ELSE N''
				END
			FROM sys.index_columns AS ic
			JOIN sys.columns AS c
				ON ic.column_id = c.column_id
				AND ic.object_id = c.object_id
			WHERE ic.object_id = si.object_id
			AND ic.index_id = si.index_id
			AND ic.key_ordinal > 0
			ORDER BY ic.key_ordinal
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS keys (key_definition)
/* Partitioning Ordinal */ OUTER APPLY (SELECT
		MAX(QUOTENAME(c.name)) AS column_name
	FROM sys.index_columns AS ic
	JOIN sys.columns AS c
		ON ic.column_id = c.column_id
		AND ic.object_id = c.object_id
	WHERE ic.object_id = si.object_id
	AND ic.index_id = si.index_id
	AND ic.partition_ordinal = 1) AS partitioning_column
/* Include list */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + QUOTENAME(c.name)
			FROM sys.index_columns AS ic
			JOIN sys.columns AS c
				ON ic.column_id = c.column_id
				AND ic.object_id = c.object_id
			WHERE ic.object_id = si.object_id
			AND ic.index_id = si.index_id
			AND ic.is_included_column = 1
			ORDER BY c.name
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS includes (include_definition)
/* Partitions */ OUTER APPLY (SELECT
		COUNT(*) AS partition_count
	   ,CAST(SUM(ps.in_row_reserved_page_count) * 8. / 1024. / 1024. AS NUMERIC(32, 1)) AS reserved_in_row_GB
	   ,CAST(SUM(ps.lob_reserved_page_count) * 8. / 1024. / 1024. AS NUMERIC(32, 1)) AS reserved_LOB_GB
	   ,SUM(ps.row_count) AS row_count
	FROM sys.partitions AS p
	JOIN sys.dm_db_partition_stats AS ps
		ON p.partition_id = ps.partition_id
	WHERE p.object_id = si.object_id
	AND p.index_id = si.index_id) AS partition_sums
/* row compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 1
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS row_compression_clause (row_compression_partition_list)
/* data compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 2
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS page_compression_clause (page_compression_partition_list)
/* data compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 3
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS columnstore_compression_clause (columnstore_compression_partition_list)
/* data compression list by partition */ OUTER APPLY (SELECT
		STUFF((SELECT
				N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
			AND p.index_id = si.index_id
			AND p.data_compression = 4
			ORDER BY p.partition_number
			FOR XML PATH (''), TYPE)
		.value('.', 'NVARCHAR(MAX)'), 1, 2, '')) AS columnstore_archive_compression_clause (columnstore_archive_compression_partition_list)


WHERE si.type IN (5)
AND sc.name + '.' + t.name = @table_tmp
ORDER BY si.index_id



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
GO

/****** Object:  StoredProcedure [dbo].[pf_trans_lobs_and_rename]    Script Date: 21.06.2022 15:37:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE or alter  PROCEDURE [dbo].[pf_trans_lobs_and_rename]
		@tbl_i		sysname
		,@tbl_i_trans sysname
as

begin try
	SET NOCOUNT ON

	declare @sql nvarchar(max)
	DECLARE @msg NVARCHAR(max);


	if exists(
		select 
			* 
		from sys.columns c 
		where object_schema_name(c.object_id) + '.'+ object_name(c.object_id) = @tbl_i 
	and c.is_identity = 1
	)
	BEGIN
	
		set @sql = 'set IDENTITY_INSERT ' + @tbl_i_trans + ' ON

	'

		select @sql = @sql + 
			'insert into ' + @tbl_i_trans + ' with(tablockx)(' +
			+ STRING_AGG(quotename(c.name), ',') + ')
			' + ' select '+  STRING_AGG(quotename(c.name), ',') + ' from  ' + @tbl_i + '
			option(maxdop 16)

	'
		from sys.tables t
		join sys.columns c
		on c.object_id = t.object_id
		where object_schema_name(t.object_id) + '.'+  t.name = @tbl_i_trans 

		set @sql = @sql + 'set IDENTITY_INSERT ' + @tbl_i_trans + ' OFF
	'

		print @sql
		exec(@sql)
	END
	ELSE
	BEGIN


		select @sql =
		--@sql + 
			'insert into ' + @tbl_i_trans + ' with(tablockx)(' +
			+ STRING_AGG(quotename(c.name), ',') + ')
			' + ' select '+  STRING_AGG(quotename(c.name), ',') + ' from  ' + @tbl_i + '
			option(maxdop 16)
	'
		from sys.tables t
		join sys.columns c
		on c.object_id = t.object_id
		where object_schema_name(t.object_id) + '.'+  t.name = @tbl_i_trans 

		print @sql

		exec(@sql)


	END


	declare @old_name sysname, @new_name sysname, @old_noschema SYSNAME

	
	
	select 
		@old_name = object_schema_name(o.object_id) + '.' + object_name(o.object_id) , 
		@new_name = object_name(o.object_id) + '_TO_DROP',
		@old_noschema = object_name(o.object_id)
	from sys.objects o
	where object_schema_name(o.object_id) + '.' + object_name(o.object_id)= @tbl_i

	print 'rename ' + @old_name + ' to ' + @new_name
	exec sp_rename @old_name, @new_name,'OBJECT'


	


	select 
		@old_name =  object_schema_name(o.object_id) + '.' + object_name(o.object_id), 
		@new_name = @old_noschema
	from sys.objects o
	where object_schema_name(o.object_id) + '.' + object_name(o.object_id)= @tbl_i_trans
	
	print 'rename ' + @old_name + ' to ' + @new_name

	exec sp_rename @old_name, @new_name,'OBJECT'


		  /*

exec pf_trans_lobs_and_rename
		@tbl_i		=	'dbo.Pkc_ncc_22_TMP'
		,@tbl_i_trans ='dbo.Pkc_ncc_22_TMP_TMP_TRANS'

		*/
END TRY
BEGIN CATCH

	SELECT
		@msg = ERROR_MESSAGE()
	RAISERROR (@msg, 20, 1) WITH LOG

END CATCH
GO

/****** Object:  StoredProcedure [dbo].[pt_create_temp_pfps]    Script Date: 21.06.2022 15:37:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE or alter  PROCEDURE [dbo].[pt_create_temp_pfps]
	@pf SYSNAME,
	@ps SYSNAME
AS
BEGIN TRY

	SET XACT_ABORT, NOCOUNT ON;

	DECLARE @l_pf SYSNAME = @pf + '_TMP';
	DECLARE @l_ps SYSNAME = @ps + '_TMP';
	DECLARE @sql NVARCHAR(max);
	DECLARE @msg NVARCHAR(max);


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
	VALUES (@log_id, 'начали. параметры: ' + @pf + ', ' + @ps , OBJECT_NAME(@@procid))
-----
--log
-----



	-- создадим темповую функцию, для datetime2 точность захардкодил в зависимости от имени функции

	SELECT
		@sql =
		N'CREATE PARTITION FUNCTION '
		+ QUOTENAME(@l_pf)
		+ N'(' +	CASE
						WHEN pf.name LIKE '%dtm2_0%' THEN 'datetime2(0)'
						WHEN pf.name LIKE '%dtm2_3%' THEN 'datetime2(3)'
						WHEN pf.name LIKE '%dtm2_7%' THEN 'datetime2(7)'
						ELSE t.name
					END

		+ N')'
		+ N' AS RANGE '
		+	 CASE
				 WHEN pf.boundary_value_on_right = 1 THEN N'RIGHT'
				 ELSE N'LEFT'
			 END
		+ ' FOR VALUES('
		+
		(
			SELECT
				STUFF(
				(
					SELECT
						N','
						+	 CASE
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') IN (N'char', N'varchar') THEN QUOTENAME(CAST(r.value AS NVARCHAR(4000)), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') IN (N'nchar', N'nvarchar') THEN N'N' + QUOTENAME(CAST(r.value AS NVARCHAR(4000)), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') = N'date' THEN QUOTENAME(FORMAT(CAST(r.value AS DATE), 'yyyy-MM-dd'), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') = N'datetime' THEN QUOTENAME(FORMAT(CAST(r.value AS DATETIME), 'yyyy-MM-ddTHH:mm:ss'), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') IN (N'datetime', N'smalldatetime') THEN QUOTENAME(FORMAT(CAST(r.value AS DATETIME), 'yyyy-MM-ddTHH:mm:ss.fff'), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') = N'datetime2' THEN QUOTENAME(FORMAT(CAST(r.value AS DATETIME2), 'yyyy-MM-ddTHH:mm:ss.fffffff'), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') = N'datetimeoffset' THEN QUOTENAME(FORMAT(CAST(r.value AS DATETIMEOFFSET), 'yyyy-MM-dd HH:mm:ss.fffffff K'), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') = N'time' THEN QUOTENAME(FORMAT(CAST(r.value AS TIME), 'hh\:mm\:ss\.fffffff'), '''') --'HH\:mm\:ss\.fffffff'
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') = N'uniqueidentifier' THEN QUOTENAME(CAST(r.value AS NVARCHAR(4000)), '''')
								 WHEN SQL_VARIANT_PROPERTY(r.value, 'BaseType') IN (N'binary', N'varbinary') THEN CONVERT(NVARCHAR(4000), r.value, 1)
								 ELSE CAST(r.value AS NVARCHAR(4000))
							 END
					FROM sys.partition_range_values AS r
					WHERE pf.[function_id] = r.[function_id]
					FOR XML PATH (''), TYPE
				)
				.value('.', 'nvarchar(MAX)'), 1, 1, N'')
		)
		+ N');'
	FROM sys.partition_functions pf
	JOIN sys.partition_parameters AS pp
		ON pp.function_id = pf.function_id
	JOIN sys.types AS t
		ON t.system_type_id = pp.system_type_id
		AND t.user_type_id = pp.user_type_id
	WHERE pf.name = @pf;

	--PRINT (@sql);
	EXEC sp_executesql @sql

-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'создали темповую функцию ' + @pf + '_TMP', OBJECT_NAME(@@procid))
-----
--log
-----


	-- создадим темповую схему
	SELECT
		@sql =
		N'CREATE PARTITION SCHEME ' + QUOTENAME(@l_ps)
		+ N' AS PARTITION ' + QUOTENAME(@l_pf)
		+ N' TO ('
		+
		(
			SELECT
				STUFF(
				(
					SELECT
						N',' + QUOTENAME(fg.name)
					FROM sys.data_spaces ds
					JOIN sys.destination_data_spaces AS dds
						ON dds.partition_scheme_id = ps.data_space_id
					JOIN sys.filegroups AS fg
						ON fg.data_space_id = dds.data_space_id
					WHERE ps.data_space_id = ds.data_space_id
					ORDER BY dds.destination_id
					FOR XML PATH (''), TYPE
				)
				.value('.', 'nvarchar(MAX)')
				, 1
				, 1
				, N'')
		)
		+ N');'
	FROM sys.partition_schemes AS ps
	JOIN sys.partition_functions AS pf
		ON pf.function_id = ps.function_id
	WHERE ps.name = @ps;

	--PRINT (@sql);
	EXEC sp_executesql @sql
-----
--log
-----
INSERT INTO log_sublog (log_id, comment, procname)
	VALUES (@log_id, 'создали темповую схему ' + @ps + '_TMP', OBJECT_NAME(@@procid))
-----
--log
-----



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

	SELECT @msg = ERROR_MESSAGE()
	RAISERROR(@msg, 20, 1) with LOG
	/*
	  SELECT
		  ERROR_NUMBER() AS ErrorNumber
		 ,ERROR_SEVERITY() AS ErrorSeverity
		 ,ERROR_STATE() AS ErrorState
		 ,ERROR_PROCEDURE() AS ErrorProcedure
		 ,ERROR_LINE() AS ErrorLine
		 ,ERROR_MESSAGE() AS ErrorMessage
		 ,XACT_STATE() as XactState;
		 
		 */
END CATCH
GO


