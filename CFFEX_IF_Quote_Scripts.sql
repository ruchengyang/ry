use CFFEX_IF_Quote
go
/*
创建CFFEX_IF_Quote 的Table，输入table名，创建table
在生成table时自动对QuoteTime字段建立unqiue clustered index
*/
ALTER proc pr_create_CFFEX_IF_Quote_Table
	@tableName varchar(max) = 'testQuoteTableTemplate'
as
begin
		declare @execstr varchar(max)
		set @execstr = 'create table ' + @tableName +
		'
		(
			SecID CHAR(6) NOT NULL,
			QuoteTime DATETIME2 NOT NULL,
			LastPrice NUMERIC(18,4),
			CumVolume NUMERIC(18,4),
			CumAmount NUMERIC(18,2),
			OpenInterest NUMERIC(18,4),
			Bid1 NUMERIC(18,4),
			Bid1Vol NUMERIC(18,4),
			Offer1 NUMERIC(18,4),
			Offer1Vol NUMERIC(18,4),
			Bid2 NUMERIC(18,4),
			Bid2Vol NUMERIC(18,4),
			Offer2 NUMERIC(18,4),
			Offer2Vol NUMERIC(18,4),
			Bid3 NUMERIC(18,4),
			Bid3Vol NUMERIC(18,4),
			Offer3 NUMERIC(18,4),
			Offer3Vol NUMERIC(18,4),
			Bid4 NUMERIC(18,4),
			Bid4Vol NUMERIC(18,4),
			Offer4 NUMERIC(18,4),
			Offer4Vol NUMERIC(18,4),
			Bid5 NUMERIC(18,4),
			Bid5Vol NUMERIC(18,4),
			Offer5 NUMERIC(18,4),
			Offer5Vol NUMERIC(18,4)
		)
		'
		if object_id(@tableName) is null
		begin
			print 'pr_create_CFFEX_IF_Quote_Table Create '+ @tableName
			exec(@execstr)
			--添加唯一聚集索引
			declare @createIndexStr varchar(max)
			set @createIndexStr = 'create unique clustered index IX_QuoteTime on ' + @tableName + ' (QuoteTime) with IGNORE_DUP_KEY'
			exec(@createIndexStr)
		end
	end
go

/*CTP Template是肯索的quote数据原始ctp模板
create table CTPTemplate(
			ttQuoteDate varchar(8),
			SecID CHAR(6) NOT NULL,
			ttEx char(10),
			ttExCode char(10),
			LastPrice NUMERIC(18,4),
			ttLastSettlePrice numeric(18,4),
			ttLastClose numeric(18,4),
			ttLastOI numeric,
			ttOpen numeric,
			ttHigh numeric,
			ttLow numeric,
			CumVolume NUMERIC(18,4),
			CumAmount NUMERIC(18,2),
			OpenInterest NUMERIC(18,4),
			ttTodayClose numeric,
			ttTodaySettle numeric,
			ttUpLimit numeric,
			ttLowLimit numeric,
			ttYY1 numeric,
			ttYY2 numeric,
			ttQuoteTime varchar(8),
			ttMs varchar(3),
			Bid1 NUMERIC(18,4),
			Bid1Vol NUMERIC(18,4),
			Offer1 NUMERIC(18,4),
			Offer1Vol NUMERIC(18,4),
			Bid2 NUMERIC(18,4),
			Bid2Vol NUMERIC(18,4),
			Offer2 NUMERIC(18,4),
			Offer2Vol NUMERIC(18,4),
			Bid3 NUMERIC(18,4),
			Bid3Vol NUMERIC(18,4),
			Offer3 NUMERIC(18,4),
			Offer3Vol NUMERIC(18,4),
			Bid4 NUMERIC(18,4),
			Bid4Vol NUMERIC(18,4),
			Offer4 NUMERIC(18,4),
			Offer4Vol NUMERIC(18,4),
			Bid5 NUMERIC(18,4),
			Bid5Vol NUMERIC(18,4),
			Offer5 NUMERIC(18,4),
			Offer5Vol NUMERIC(18,4),
			ttAvg numeric,
			ttBusinessDay varchar(8)
)
go
*/

/*导入单个肯索CFFEX_IF_Quote文件
适用于2010年4月至2015年1月数据
	csvAddr,文件地址名
	tableName,表名
*/

alter proc pr_bulk_Insert_IF_Quote_From_KenSuo_CTP
	@csvAddr varchar(max) ,
	@tableName varchar(max) 
as
begin
	create table #temp(
			ttQuoteDate varchar(8),
			SecID CHAR(6) NOT NULL,
			ttEx char(10),
			ttExCode char(10),
			LastPrice NUMERIC(18,4),
			ttLastSettlePrice numeric(18,4),
			ttLastClose numeric(18,4),
			ttLastOI numeric,
			ttOpen numeric,
			ttHigh numeric,
			ttLow numeric,
			CumVolume NUMERIC(18,4),
			CumAmount NUMERIC(18,2),
			OpenInterest NUMERIC(18,4),
			ttTodayClose numeric,
			ttTodaySettle numeric,
			ttUpLimit numeric,
			ttLowLimit numeric,
			ttYY1 numeric,
			ttYY2 numeric,
			ttQuoteTime varchar(8),
			ttMs varchar(3),
			Bid1 NUMERIC(18,4),
			Bid1Vol NUMERIC(18,4),
			Offer1 NUMERIC(18,4),
			Offer1Vol NUMERIC(18,4),
			Bid2 NUMERIC(18,4),
			Bid2Vol NUMERIC(18,4),
			Offer2 NUMERIC(18,4),
			Offer2Vol NUMERIC(18,4),
			Bid3 NUMERIC(18,4),
			Bid3Vol NUMERIC(18,4),
			Offer3 NUMERIC(18,4),
			Offer3Vol NUMERIC(18,4),
			Bid4 NUMERIC(18,4),
			Bid4Vol NUMERIC(18,4),
			Offer4 NUMERIC(18,4),
			Offer4Vol NUMERIC(18,4),
			Bid5 NUMERIC(18,4),
			Bid5Vol NUMERIC(18,4),
			Offer5 NUMERIC(18,4),
			Offer5Vol NUMERIC(18,4),
			ttAvg numeric,
			ttBusinessDay varchar(8)
	)

	exec('
		BULK INSERT #temp
		FROM ''' + @csvAddr +
		''' WITH 
		(	
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''\n'',
			FIRSTROW = 2
		)')

	exec('
	INSERT INTO ' + @tableName + '(SecID,QuoteTime,LastPrice,CumVolume,CumAmount,OpenInterest,Bid1,Bid1Vol,Offer1,Offer1Vol) SELECT SecID, cast(ttQuoteDate + '' '' + ttQuoteTime + ''.'' + ttMs as datetime) as QuoteTime ,LastPrice,CumVolume,CumAmount,OpenInterest,Bid1,Bid1Vol,Offer1,Offer1Vol FROM #temp order by QuoteTime'
	)
	drop table #temp
end
go


/*导入单个肯索 quote s数据，非CTP格式，由于数据格式不同，所以需要新的方式
适用于2015年2月以后数据
	csvAddr,文件地址名
	tableName,表名
*/
use CFFEX_IF_Quote
go
alter proc pr_bulk_Insert_IF_Quote_From_Kensuo_NonCTP
	@csvAddr varchar(max) ,
	@tableName varchar(max)
as
begin
	create table #temp(
			ttEx char(2),
			SecID CHAR(6) NOT NULL,
			ttQuoteTime varchar(19),
			ttMs varchar(3),
			LastPrice NUMERIC(18,4),
			OpenInterest NUMERIC(18,4),
			OpenInterestDelta NUMERIC(18,4),
			Amount NUMERIC(18,4),
			CumAmount NUMERIC(18,2),
			Volume numeric(18,4),
			CumVolume NUMERIC(18,4),
			Bid1 NUMERIC(18,4),
			Offer1 NUMERIC(18,4),
			Bid1Vol NUMERIC(18,4),
			Offer1Vol NUMERIC(18,4),
			ttOpen numeric,
			ttHigh numeric,
			ttLow numeric,
			ttLastClose numeric,
			ttLastSettle numeric,	
	)

	exec('
		BULK INSERT #temp
		FROM ''' + @csvAddr +
		''' WITH 
		(	
			FIELDTERMINATOR = '','',
			ROWTERMINATOR = ''\n'',
			FIRSTROW = 2
		)')

	exec('
	INSERT INTO ' + @tableName + '(SecID,QuoteTime,LastPrice,CumVolume,CumAmount,OpenInterest,Bid1,Bid1Vol,Offer1,Offer1Vol) SELECT SecID, cast(ttQuoteTime + ''.'' + ttMs as datetime2) as QuoteTime ,LastPrice,CumVolume,CumAmount,OpenInterest,Bid1,Bid1Vol,Offer1,Offer1Vol FROM #temp order by QuoteTime'
	)
	drop table #temp
end
go
