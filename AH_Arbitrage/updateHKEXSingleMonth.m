function result = updateHKEXSingleMonth(year,month)
    % year,month: int
    beginDate = datetime(year,month,1);
    endDate = datetime(year,month,eomday(year,month));
    
    w = windmatlab;
    [NUM,TXT,RAW]=xlsread('AH股.xlsx');
    
    for i = 2:size(RAW,1)
        conn = database.ODBCConnection('test','sa','sa');
        secID = RAW{i,1};
        fprintf('%d %s\n',i,secID);
        %从wind读取数据
        [data,codes,fields,times,errorid,reqid]=w.wsd(secID,'trade_code,sec_name,open,high,low,close,volume,amt,adjfactor',datestr(beginDate,'yyyy-mm-dd'),datestr(endDate,'yyyy-mm-dd'),'TradingCalendar=HKEX','Period=M');
        
        %去掉nan形成最终data
        index = ~cellfun(@isnan,data(:,6));
        data = data(index,:);
        times = datestr(times(index,:),'yyyy-mm-dd');
        data = [strcat(data(:,1),'.HK'),data(:,2),cellstr(times),data(:,3:end)];


        %删除原数据
        sql = strcat('delete from HKEX.dbo.HKEXMonthlyBar where secID = ''', strcat(secID,'.HK'), ''' and convert(char(7),BarTime,120) = ''', datestr(beginDate,'yyyy-mm'),'''');
        exec(conn,sql);
        
        %插入新数据
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
    
    %插入港币汇率
    conn = database.ODBCConnection('test','sa','sa');
    [data,codes,fields,times,errorid,reqid]=w.wsd('HKDCNY.FX','trade_code,close',beginDate,endDate,'TradingCalendar=HKEX','Period=M');
    data = num2cell(data);
    index = ~cellfun(@isnan,data(:,2));
    data = data(index,:);
    data(:,1) = {'HKDCNY.FX'};
    times = datestr(times(index,:),'yyyy-mm-dd');
    data = [data(:,1),cellstr(times),data(:,2)];
    
    %删除原数据
    sql = strcat('delete from HKEX.dbo.HKDCNY where convert(char(7),BarTime,120) = ''', datestr(beginDate,'yyyy-mm'),'''');
    exec(conn,sql);

    try
        fastinsert(conn,'HKEX.dbo.HKDCNY',{'SecID','BarTime','ClosePrice'},data)
    catch ME
        % 过虑掉重复插入的报错信息
        fprintf('%s\n%s\n','HKDCNY',ME.message);
        close(conn);
    end
    
    fprintf('%s %s %s\n','Insert', 'HKDCNY.CNY','data into HKEX.dbo.HKDCNY');
    close(conn);
end