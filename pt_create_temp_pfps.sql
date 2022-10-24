SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [dbo].[pt_create_temp_pfps]
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
