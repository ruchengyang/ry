use CFFEX_IF_Quote
truncate table TestQuoteTableTemplate
create unique clustered index IX_Time on IF1001(QuoteTime) with IGNORE_DUP_KEY 
exec pr_bulk_Insert_IF_Quote_From_KenSuo_CTP 'C:\database\raw\CFFEX_IF_Quote\Kensuo\IF_201004\IF1005_20100416.csv', 'TestQuoteTableTemplate'

select * from TestQuoteTableTemplate



declare @year int
declare @mon int
declare @tableName varchar(max)

set @year = 13
set @mon = 1


while @year <= 18
begin
	while @mon <= 12
	begin
		set @tableName =  'IF' + cast(@year as varchar) + right('00'+cast(@mon as varchar),2)

		if exists(select * from sysindexes where id=object_id(@tableName) and name='IX_Time') 
			print '´æÔÚ' +@tableName
		else 
		begin
			exec( 'create unique clustered index IX_Time on ' + @tableName+'(QuoteTime) with IGNORE_DUP_KEY' )
			print  'create unique clustered index IX_Time on ' + @tableName+'(QuoteTime) with IGNORE_DUP_KEY'
		end
		set @mon = @mon + 1
	end
	set @mon = 1
	set @year = @year + 1
end