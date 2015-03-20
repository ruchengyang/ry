use HKEX
go

alter proc pr_create_HKEX_Monthly_Table
	@tableName varchar(max) = 'HKEXMonthlyBar'
as
begin
		declare @execstr varchar(max)
		set @execstr = 'create table ' + @tableName +
		'
		(
			SecID CHAR(7) NOT NULL,
			ChiName varchar(20) NOT NULL,
			BarTime DATETIME NOT NULL,
			OpenPrice numeric(18,4),
			HighPrice numeric(18,4),
			LowPrice numeric(18,4),
			ClosePrice numeric(18,4),
			Volume numeric(18,4),
			Amount numeric(18,4),
			AdjFactor numeric(18,4)
		)
		'
		if object_id(@tableName) is null
		begin
			print 'pr_create_HKEX_Monthly_Table Create '+ @tableName
			exec(@execstr)
			--添加唯一聚集索引
			declare @createIndexStr varchar(max)
			set @createIndexStr = 'create unique clustered index IX_BarTime on ' + @tableName + ' (SecID,BarTime) with IGNORE_DUP_KEY'
			exec(@createIndexStr)
		end
	end
go

--创建HKDCNY表
create table HKDCNY (
	SecID CHAR(9) NOT NULL,
	BarTime DATETIME NOT NULL,
	ClosePrice numeric(18,4)
	)
create unique clustered index IX_BarTime on HKDCNY (BarTime) with IGNORE_DUP_KEY


use HKEX
exec pr_create_HKEX_Monthly_Table
go
