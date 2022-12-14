USE [MDWH_RAW]
GO
/****** Object:  StoredProcedure [dbo].[pf_merge_split_partitions]    Script Date: 11.08.2022 9:56:01 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or ALTER   PROCEDURE [dbo].[pf_merge_split_partitions]
		 @pf	sysname 
		,@ps	sysname 
		,@dateadd_month int
as 
BEGIN TRY

SET XACT_ABORT, NOCOUNT ON;

/*


процедура переноса секций в архив, запускает из себя доп.процедуры по ребилду индексов и переливки данных
		

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




	select @date_cur = dateadd(month,-@dateadd_month ,@getdate)

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
	WHERE i.type IN( 1,5,0)
	AND partition_number = @pt_number_cur

	--SELECT * FROM #tbls_to_recreate




	WHILE EXISTS(SELECT * FROM #tbls_to_recreate WHERE worked = 0)
	BEGIN

		SELECT TOP 1 
			@tbl_i=  table_name
		FROM #tbls_to_recreate ttr
		WHERE worked = 0

		--собираем лобы и хипы
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

					UNION all

					SELECT DISTINCT
						s.[name]  + '.' + t.[name] AS [object_name]
						,'HEAP' as type_desc	
						,0 as used_pages

					FROM sys.tables AS t WITH (NOLOCK)
					INNER JOIN sys.schemas AS s WITH (NOLOCK)
						ON t.[schema_id] = s.[schema_id]
					INNER JOIN sys.indexes AS i WITH (NOLOCK)
						ON t.[object_id] = i.[object_id]
					INNER JOIN sys.partitions AS p WITH (NOLOCK)
						ON i.[object_id] = p.[object_id]
							AND i.index_id = p.index_id
					INNER JOIN sys.allocation_units AS au WITH (NOLOCK)
						ON p.hobt_id = au.container_id
					INNER JOIN sys.filegroups AS fg WITH (NOLOCK)
						ON au.data_space_id = fg.data_space_id
					INNER JOIN sys.master_files AS mf
						ON fg.data_space_id = mf.data_space_id
							AND mf.database_id = DB_ID()
					LEFT JOIN sys.partition_schemes AS ps WITH (NOLOCK)
						ON i.data_space_id = ps.data_space_id
					LEFT JOIN sys.partition_functions AS pf WITH (NOLOCK)
						ON ps.function_id = pf.function_id
					LEFT JOIN sys.partition_range_values AS prv WITH (NOLOCK)
						ON pf.function_id = prv.function_id
							AND p.partition_number =
							CASE
								WHEN pf.boundary_value_on_right = 1 THEN prv.boundary_id + 1
								ELSE prv.boundary_id
							END
					WHERE i.type IN (0)
					and s.[name]  + '.' + t.[name] = @tbl_i			
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
