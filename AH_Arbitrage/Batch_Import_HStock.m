% 批量导入港股月线数据(未复权，含有复权系数)至sql server

%%
cd('C:\trading\AH_Arbitrage')
beginDate = '2000-01-01';
endDate = '2015-02-28';
w = windmatlab;
[NUM,TXT,RAW]=xlsread('AH股.xlsx');

 
%形成2000-1-1至2015-3-16号的港股历史数据库
for i = 2:size(RAW,1)
    conn = database.ODBCConnection('test','sa','sa');
    secID = RAW{i,1};
    fprintf('%d %s\n',i,secID);
    %从wind读取数据
    [data,codes,fields,times,errorid,reqid]=w.wsd(secID,'trade_code,sec_name,open,high,low,close,volume,amt,adjfactor',beginDate,endDate,'TradingCalendar=HKEX','Period=M');
    
    %去掉nan形成最终data
    index = ~cellfun(@isnan,data(:,6));
    data = data(index,:);
    times = datestr(times(index,:),'yyyy-mm-dd');
    data = [strcat(data(:,1),'.HK'),data(:,2),cellstr(times),data(:,3:end)];
    
    
    %插入数据
    try
        fastinsert(conn,'HKEX.dbo.HKEXMonthlyBar',{'SecID','ChiName','BarTime','OpenPrice','HighPrice','LowPrice','ClosePrice','Volume','Amount','AdjFactor'},data)
    catch ME
        % 过虑掉重复插入的报错信息
        fprintf('%s\n%s\n',secID,ME.message);
        close(conn);
        continue;
    end
        
    fprintf('%s %s %s\n','Insert', secID,'data into HKEX.dbo.HKEXMonthlyBar');
    close(conn);
end

% 读入人民币兑港币数据
conn = database.ODBCConnection('test','sa','sa');
[data,codes,fields,times,errorid,reqid]=w.wsd('HKDCNY.FX','trade_code,close',beginDate,endDate,'TradingCalendar=HKEX','Period=M');
data = num2cell(data);
index = ~cellfun(@isnan,data(:,2));
data = data(index,:);
data(:,1) = {'HKDCNY.FX'};
times = datestr(times(index,:),'yyyy-mm-dd');
data = [data(:,1),cellstr(times),data(:,2)];
try
    fastinsert(conn,'HKEX.dbo.HKDCNY',{'SecID','BarTime','ClosePrice'},data)
catch ME
    % 过虑掉重复插入的报错信息
    fprintf('%s\n%s\n','HKDCNY',ME.message);
    close(conn);
end

fprintf('%s %s %s\n','Insert', 'HKDCNY.CNY','data into HKEX.dbo.HKDCNY');
close(conn);
