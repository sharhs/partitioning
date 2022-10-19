create or alter procedure maintenance.asw_system_versioning_toggle
		@toggle bit 


as
BEGIN TRY

	SET XACT_ABORT ON;
	
	DECLARE @msg NVARCHAR(max);


	declare @sql nvarchar(max)
	, @tbl sysname
	,@tbl_hist sysname
	,@col_st sysname
	,@col_et sysname


	if @toggle = 0
	begin
		while exists(
					select * 
					from #temporal 
					where to_off = 0
					)
		BEGIN

			select @tbl = objname	
			,@tbl_hist = objname_temporal	
			from #temporal
			where to_off = 0

			set @sql = 'ALTER TABLE '+@tbl+' SET (SYSTEM_VERSIONING = OFF)'

			print @sql
			exec(@sql)

			set @sql = 'ALTER TABLE '+@tbl+' DROP PERIOD FOR SYSTEM_TIME'

			print @sql
			exec(@sql)


			update #temporal
			set to_off = 1
			where objname = @tbl 

		end
	end


	if @toggle = 1
	begin
		while exists(
					select * 
					from #temporal 
					where to_on = 0
					)
		BEGIN

			select @tbl = objname	
				  ,@tbl_hist = objname_temporal	
				  ,@col_st = name
			from #temporal
			where to_on = 0
			and  generated_always_type = 1

			select 
				@col_et = name
			from #temporal
			where to_on = 0
			and generated_always_type = 2
			and objname_temporal	=@tbl_hist 
			and objname	= @tbl 


			set @sql = 'ALTER TABLE '+@tbl+' ADD PERIOD FOR SYSTEM_TIME (['+@col_st+'], ['+@col_et+'])'

			print @sql
			exec(@sql)

			set @sql = 'ALTER TABLE '+@tbl+' SET (SYSTEM_VERSIONING = on( HISTORY_TABLE = '+@tbl_hist+', DATA_CONSISTENCY_CHECK = ON ))'
		
			print @sql
			exec(@sql)

			update #temporal
			set to_on = 1
			where objname = @tbl 


		end 


	end



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