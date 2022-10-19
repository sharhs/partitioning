USE MDWH_RAW
GO
create or alter procedure maintenance.asw_transfer_partitions_to_history
	@dateadd_month int --указываем период для работы
as
BEGIN TRY

	SET XACT_ABORT ON;
	
	DECLARE @msg NVARCHAR(max);

	/*

процедура набирает таблицы из схем секционирования и переносит нужную секию в архивную файловую группу History
далее для примера взяты конкретные номера секций (61,62,63)


1. подготовка темповые объекты 

выполняется процедурами 
pf_create_tmp_tables_indexes - создает таблицу и все роустор индексы, если есть
pf_create_temp_columnstore_indexes - создает колумнстор индексы на созданную таблицу, если есть

создаются три темповые таблицы с окончанием [partition_number]_ASW, а так же все индексы на ней
примеры именования таблиц:
[raw_tiburon].[voucher_transaction_61_TMP]
[raw_tiburon].[voucher_transaction_62_TMP]
[raw_tiburon].[voucher_transaction_63_TMP]

2. перенос 
переносы секций выполняется процедурой pf_merge_split_partitions

происходит переключение перемещаемой в архив секции и двух соседних слева и справа в созданные таблицы:
alter table [raw_tiburon].[voucher_transaction] switch partition 61 to [raw_tiburon].[voucher_transaction_61_TMP]
alter table [raw_tiburon].[voucher_transaction] switch partition 62 to [raw_tiburon].[voucher_transaction_62_TMP]
alter table [raw_tiburon].[voucher_transaction] switch partition 63 to [raw_tiburon].[voucher_transaction_63_TMP]
переключение выполняется с параметрами max_duration = 1 minutes, abort_after_wait = blockers

после переключения секций, они пустые и их можно мерджить и сплитить - это выполняется через alter partition function split/merge

далее выполняется ребилд таблицы [raw_tiburon].[voucher_transaction_62_TMP] на архивную фг, что бы ее можно было переключить обратно в таблицу в нужную секцию
в случае, если таблица содержит lob-данные (при ребилде индекса не происходит их перемещения в другую фг), создается доп таблица 
[raw_tiburon].[voucher_transaction_62_TMP_TO_DROP] на архивной секции и запускается переливка данных в нее

после того, как все темповые таблицы перестроены/перелиты в архивную фг, выполняется обратное переключение с параметрами max_duration = 1 minutes, abort_after_wait = blockers

3. выполняется удаление темповых объектов


	*/

	SET NOCOUNT ON;
	DECLARE @tbl SYSNAME
		   ,@pf SYSNAME
		   ,@ps SYSNAME
		   ,@pf_created BIT
		   ,@ps_created BIT
		   ,@table_created BIT
		   ,@indexes_created BIT
		   ,@sql NVARCHAR(MAX)
		   ,@error INT = 0


	DROP TABLE IF EXISTS #tbls_pfps
	DROP TABLE IF EXISTS #pfps
	DROP TABLE IF EXISTS #part_info
	DROP TABLE IF EXISTS #tmp_tables
	DROP TABLE IF exists #temporal

	DECLARE @i_table SYSNAME
		   ,@i_ps_name SYSNAME
		   ,@i_ps_colname SYSNAME
		   ,@i_pnumber bigint

	declare @pt_number_cur		int
			,@pt_number_left	int
			,@pt_number_right	int
			,@getdate			datetime =  DATEADD(m, DATEDIFF(m, 0, GETDATE()), 0)  --		 '2022-12-01 00:00:00'	
			,@date_cur			datetime
			,@date_left			datetime
			,@date_right		datetime


	select @date_cur = dateadd(month,-@dateadd_month,@getdate)

	set @date_left = dateadd(month,-1,@date_cur)
	set @date_right = dateadd(month,1,@date_cur)




	-----
	--log
	-----
	DECLARE @log_id INT

	INSERT INTO log_headlog (log_date, host, username, session_id, procname)
		VALUES (GETDATE(), HOST_NAME(), ORIGINAL_LOGIN(), @@spid , 'pf_fg_to_history')
	SET @log_id = SCOPE_IDENTITY()

	INSERT INTO log_sublog (log_id, comment,procname)
		VALUES (@log_id, 'start slip/merge','pf_fg_to_history')
	-----
	--log
	-----

	/*
	набираем список таблиц, с которым будем работать
	на данный момент процедура собирает все индексы с типом 
	1 - кластерный роустор
	5 - кластерный колумнстор
	0 - куча, добавлено
	*/

	SELECT DISTINCT
		s.[name] AS [schema_name]
	   ,t.[name] AS [object_name]
	   ,t.object_id AS objectid
	   ,p.[rows]
	   ,p.partition_number
	   ,fg.[name] AS fg_name
	   ,ps.[name] AS ps_name
	   ,pf.[name] AS pf_name
	   ,(SELECT
				c.name
			FROM sys.index_columns AS ic
			INNER JOIN sys.columns AS c
				ON ic.[object_id] = c.[object_id]
				AND ic.column_id = c.column_id
			WHERE ic.[object_id] = i.[object_id]
			AND ic.index_id = i.index_id
			AND ic.partition_ordinal = 1)
		AS ps_colname

	   ,prv.[value] AS bound_value
	   ,0 AS IsProcessed
  
	INTO #part_info

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
	WHERE i.type IN (1, 5, 0)
	AND ps.[name] like '%ASW%'
	ORDER BY t.[name], p.[partition_number]

	--select * from #part_info	where partition_number = 62  order by rows desc, object_name,partition_number
	--select * from #part_info	where partition_number = 62  order by object_name,partition_number
	--select * into ##part_info from #part_info	where partition_number = 62  order by object_name,partition_number



	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'отключаем от темпоральных таблиц и дропаем систем период','pf_fg_to_history')
	-----
	--log
	-----
	
	;with cte as(	
	select distinct 
		[schema_name]+'.'+ [object_name] as tname
		,objectid
		,0 as worked
	from #part_info)
	select 
		OBJECT_SCHEMA_NAME(c.object_id) + '.' + OBJECT_NAME(c.object_id) objname
		,OBJECT_SCHEMA_NAME(t.history_table_id) + '.' + OBJECT_NAME(t.history_table_id)  objname_temporal
		,c.name
		,c.generated_always_type
		,c.generated_always_type_desc
		, tl.worked as to_off
		, tl.worked as to_on
	into #temporal
	from sys.periods p
	join sys.columns c
		on c.column_id = p.end_column_id
		or c.column_id = p.start_column_id
	join sys.tables t
		on c.object_id = t.object_id
		and p.object_id = t.object_id
	join CTE tl
		on t.object_id = tl.objectid
	where t.temporal_type=2




	exec maintenance.asw_system_versioning_toggle @toggle = 0 --отключить темпорал

	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'отключили от темпоральных таблиц и дропаем систем период','pf_fg_to_history')
	-----
	--log
	-----


	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'собрали список таблиц, функций и схем к работе','pf_fg_to_history')
	-----
	--log
	-----


	SELECT DISTINCT
		ps_name
	   ,ps_colname
	   ,pf_name
	   ,[schema_name]
	   ,[object_name]
	   ,0 AS IsProcessed INTO #tbls_pfps
	FROM #part_info


	SELECT DISTINCT
		pf_name PartitionFunction
	   ,ps_name PartitionScheme
	   ,0 AS created
	   ,0 AS splited INTO #pfps
	FROM #tbls_pfps


	SELECT  
		schema_name +'.' + object_name	  AS table_name
		,partition_number
		,fg_name
		,ps_name
		,pf_name
		,bound_value
		,ps_colname
		,0 AS isProcessed
	INTO #tmp_tables
	from #part_info
	where bound_value IN( @date_cur,@date_left,@date_right)
	ORDER BY table_name
		,partition_number
		,ps_name
	
	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'создаем временные индексы','pf_fg_to_history')
	-----
	--log
	-----


	WHILE EXISTS (SELECT
			*
		FROM #tmp_tables
		WHERE isProcessed = 0)
	BEGIN
	BEGIN TRY

		SELECT TOP 1
			@i_table = table_name 
		   ,@i_ps_name = fg_name
		   ,@i_ps_colname = ps_colname
		   ,@i_pnumber = partition_number
		FROM #tmp_tables
		WHERE isProcessed = 0


		EXEC pf_create_tmp_tables_indexes @i_table
										 ,@i_ps_name
										 ,@i_ps_colname
										 ,@i_pnumber

		EXEC pf_create_temp_columnstore_indexes @i_table
											   ,@i_ps_name
											   ,@i_ps_colname
											   ,@i_pnumber


	END TRY
	BEGIN CATCH	 
	END CATCH

		UPDATE #tmp_tables
		SET isProcessed = 1
		WHERE table_name  = @i_table
		AND partition_number = @i_pnumber 


	END

	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'создали','pf_fg_to_history')
	-----
	--log
	-----


	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'выполняем переносы секций в архиную фг','pf_fg_to_history')
	-----
	--log
	-----
	WHILE EXISTS (SELECT
			*
		FROM #pfps
		WHERE splited = 0)

	BEGIN
	BEGIN TRY

		SELECT TOP 1
			@pf = PartitionFunction
		   ,@ps = PartitionScheme
		FROM #pfps
		WHERE splited = 0
		ORDER BY PartitionFunction


		EXEC pf_merge_split_partitions @pf
									  ,@ps
									  ,@dateadd_month
	END TRY
	BEGIN CATCH

	END CATCH

		 
		UPDATE #pfps
		SET splited = 1
		WHERE @pf = PartitionFunction
		AND @ps = PartitionScheme

	END


	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'выполнили','pf_fg_to_history')
	-----
	--log
	-----






	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'включаем версионность и подписываем темпоралки','pf_fg_to_history')
	-----
	--log
	-----


		exec maintenance.asw_system_versioning_toggle @toggle = 1 --включить темпорал

	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'включили','pf_fg_to_history')
	-----
	--log
	-----

	/*

	--update #pfps set splited = 0

	
	DROP TABLE IF EXISTS #tmp_tables_to_drop
	DROP TABLE IF EXISTS #pf_pf_to_drop

	
	SELECT DISTINCT
		object_schema_name(t.object_id) + '.' + object_name(t.object_id) tbl_name
	   ,0 AS dropped 
	INTO #tmp_tables_to_drop
	FROM sys.tables t
	where name like '%_TMP%'
	order by 1

	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'удаляем временные объекты после себя','pf_fg_to_history')
	-----
	--log
	-----
	WHILE EXISTS (SELECT
			*
		FROM #tmp_tables_to_drop tttd
		WHERE tttd.dropped = 0)
	BEGIN

		SELECT TOP 1
			@tbl = tttd.tbl_name
		FROM #tmp_tables_to_drop tttd
		WHERE tttd.dropped = 0

		SET @sql = 'drop table ' + @tbl
		PRINT @sql
		EXEC (@sql)

		UPDATE #tmp_tables_to_drop
		SET dropped = 1
		WHERE tbl_name = @tbl

	END


	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'удалили', 'pf_fg_to_history')
	-----
	--log
	-----
	*/


	-----
	--log
	-----
	INSERT INTO log_sublog (log_id, comment, procname)
		VALUES (@log_id, 'финиш','pf_fg_to_history')
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