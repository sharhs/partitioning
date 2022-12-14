USE [MDWH_RAW]
GO
/****** Object:  StoredProcedure [dbo].[pf_trans_lobs_and_rename]    Script Date: 11.08.2022 9:56:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



create or ALTER    PROCEDURE [dbo].[pf_trans_lobs_and_rename]
		@tbl_i		sysname
		,@tbl_i_trans sysname
as

begin try

/*
выполняет переливку данных, в случае работы с таблицей, где есть lob'ы, в доп.тпблицу и выполняет нужные переименования таблицы
*/
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
