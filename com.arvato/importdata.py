import random
import logging
import xlrd
import time, datetime
import xlutils.copy

import logging.handlers  
from logging.handlers import RotatingFileHandler

import os
import pymysql
import pymysql.cursors


Rthandler = RotatingFileHandler('datav_import.log', maxBytes=10*1024*1024,backupCount=5)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)


#进行日期转换
def getdate(date):
    #if isinstance(date, float):
    #    date = int(date)
    #d = datetime.date.fromordinal(__s_date + date)
    pythondate  = date - datetime.timedelta(days = 1462)
    return pythondate.strftime("%d-%b")
    

def getTitleSheet(excelfile):

    
    table = data.sheet_by_index(0)
    nrows = table.nrows
    ncols = table.ncols
    colnames =  table.row_values(0)
    titleList =[excelfile[13:23]]
    #print(table.cell(1,2).value)

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        print(str(row[1]))
        
        titleList.append(row[1])
        #if row:
        #    app = {}
        #    for i in range(len(colnames)):
        #        app[colnames[i]] = row[i] 
        #    list.append(app)
    #list.append(nrows)
    #list.append(time.strftime('%Y-%m-%d %H:%M:%S'))
    #app = {}
    #for i in range(1,len(colnames)):
    #    list.append(colnames[i])
    #print(list)   

    return titleList

def getCfrSheet(excelfile):
    
    table = data.sheet_by_index(1)
    nrows = table.nrows
    ncols = table.ncols
    
    cfrList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)

        cfrTup=(row[0],row[1],row[2])
        print(cfrTup)
        cfrList.append(cfrTup)

    return cfrList


def getSaleSheet(excelfile):
    
    table = data.sheet_by_index(2)
    nrows = table.nrows
    ncols = table.ncols
    colnames =  table.row_values(0)
    saleList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        app = {}
        for i in range(len(colnames)):
                #print(colnames[i]+":"+str(row[i]))
                #print()
            app[colnames[i]] = str(row[i]) 
        
        #cfrTup=(row[0],row[1],row[2])
        print(app)
        saleList.append(app)

    return saleList


def getForeCastSheet(excelfile):
    
    table = data.sheet_by_index(3)
    nrows = table.nrows
    ncols = table.ncols
    
    foreCastList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        #d = xlrd.xldate.xldate_as_datetime(table.cell(rownum,0).value, 1).strftime('%Y-%m-%d') 
        d = xlrd.xldate.xldate_as_datetime(table.cell(rownum,0).value, 1)
        castTup=(getdate(d),row[1],row[2])
        print(castTup)
        foreCastList.append(castTup)

    return foreCastList


def clearData(connection,dailyOrder):
    
    
    
    # 执行sql语句
    
    with connection.cursor() as cursor:
        #for rownum in range(0,len(titleList)):
            # 执行sql语句，插入记录
        print(titleList)
        sql = 'delete from datav_config where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_cfr_data where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_salereport_region where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_salereport_category where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_forecast_actual where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        
        
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()


def importTitleSheet(connection,dailyOrder,titleList):
    
    
    # 执行sql语句
    with connection.cursor() as cursor:
        #for rownum in range(0,len(titleList)):
            # 执行sql语句，插入记录
        sql = '''INSERT INTO datav_config (dailyorder,totalworkingday,remainworkingday,
                    FYST,gs,timeprocess,
                    field1,field2,field3,
                    field4,field5,field6,
                    field7,field8,field9,
                    field10) VALUES (%s, %s,%s, 
                    %s,%s, %s,
                    %s, %s,%s, 
                    %s,%s, %s,
                    %s, %s,%s, %s)'''
        fuck = float('%.2f'%(titleList[4]*100)) 
        timeprocess = str(int(fuck))+'%'

        num  = cursor.execute(sql, (dailyOrder, str(int(titleList[2])),str(int(titleList[3])),
                                    str(titleList[5]),str(titleList[1]),timeprocess,
                                    titleList[6],titleList[7],titleList[8],
                                    titleList[9],titleList[10],titleList[11],
                                    titleList[12],titleList[13],titleList[14],titleList[15]));
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()
     
    return num 

def importCfrSheet(connection,dailyOrder,cfrList):
    
    # 执行sql语句
    with connection.cursor() as cursor:
        for rownum in range(0,len(cfrList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(cfrList[rownum])
            getCfr ="SELECT key2 FROM datav_dictionary where display = %s and key2 like %s"
            cursor.execute(getCfr, (cfrList[rownum][0],'1%'));
            result = cursor.fetchone()
            print(cfrList[rownum][1])
            sql = 'INSERT INTO datav_cfr_data (dailyorder,daily,mtd,cfr) VALUES (%s, %s,%s, %s)'
            daily = str(cfrList[rownum][1]*100)[0:5]+'%'
            mtd = str(cfrList[rownum][2]*100)[0:5]+'%'
            num  = cursor.execute(sql, (dailyOrder,daily ,mtd,result['key2']))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()
     
    return num 


def importSaleSheet(connection,dailyOrder,saleList):
    
    # 执行sql语句
    with connection.cursor() as cursor:
        
        #region
        for rownum in range(0,8):
            # 执行sql语句，插入记录cfrList[rownum]
            print(saleList[rownum])
            getCfr ="SELECT key2 FROM datav_dictionary where display = %s and key2 like %s"
            cursor.execute(getCfr, (saleList[rownum]['Region'],'3%'));
            result = cursor.fetchone()
            print('fucktest : '+str(saleList[rownum]['Yesterdaysales']))
            sql = '''INSERT INTO datav_salereport_region (dailyorder,region,
            Yesterdaysales,AvgdailyMTG,CarryOver,
            MTDAct,MTDST,MTDAch,
            QAct,QST,QAch,
            YTDAct,YTDST,YTDAch,wptarget) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s,%s)'''
            
            Wptarget=str(saleList[rownum]['Wptarget'])
            if Wptarget=='':
                Wptarget=str(saleList[rownum]['MTDST'])
            
                    
            num  = cursor.execute(sql, (dailyOrder, result['key2'],
                                        str(saleList[rownum]['Yesterdaysales']),str(saleList[rownum]['AvgdailyMTG']),str(saleList[rownum]['CarryOver']),
                                        str(saleList[rownum]['MTDAct']),str(saleList[rownum]['MTDST']),str(saleList[rownum]['MTDAch.']),
                                        str(saleList[rownum]['QAct.']),str(saleList[rownum]['QST']),str(saleList[rownum]['QAch.']),
                                        str(saleList[rownum]['YTDAct']),str(saleList[rownum]['YTDST']),str(saleList[rownum]['YTDAch.']),Wptarget))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        #category
        for rownum in range(9,len(saleList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(saleList[rownum])
            getCfr ="SELECT key2 FROM datav_dictionary where display = %s and key2 like %s"
            cursor.execute(getCfr, (saleList[rownum]['Region'],'4%'));
            result = cursor.fetchone()
            print('fucktest : '+str(saleList[rownum]['Yesterdaysales']))
            sql = '''INSERT INTO datav_salereport_category (dailyorder,category,
            Yesterdaysales,AvgdailyMTG,CarryOver,
            MTDAct,MTDST,MTDAch,
            QAct,QST,QAch,
            YTDAct,YTDST,YTDAch,wptarget) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s,%s,%s)'''
            
            Wptarget=str(saleList[rownum]['Wptarget'])
            if Wptarget=='':
                Wptarget=str(saleList[rownum]['MTDST'])
            num  = cursor.execute(sql, (dailyOrder, result['key2'],
                                        str(saleList[rownum]['Yesterdaysales']),str(saleList[rownum]['AvgdailyMTG']),str(saleList[rownum]['CarryOver']),
                                        str(saleList[rownum]['MTDAct']),str(saleList[rownum]['MTDST']),str(saleList[rownum]['MTDAch.']),
                                        str(saleList[rownum]['QAct.']),str(saleList[rownum]['QST']),str(saleList[rownum]['QAch.']),
                                        str(saleList[rownum]['YTDAct']),str(saleList[rownum]['YTDST']),str(saleList[rownum]['YTDAch.']),Wptarget))


    connection.commit()
     
    return num 


def importForeCastSheet(connection,dailyOrder,foreCastList):
    
    # 执行sql语句
    with connection.cursor() as cursor:
        for rownum in range(0,len(foreCastList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(foreCastList[rownum])
            sql = 'INSERT INTO datav_forecast_actual (dailyorder,forecast,actual,daynumber) VALUES (%s, %s,%s, %s)'
            num  = cursor.execute(sql, (dailyOrder, foreCastList[rownum][1],foreCastList[rownum][2],foreCastList[rownum][0]))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
     
    return num 


if __name__ == '__main__':

    #excelfile="D:\\ftpfiles\\datav_import_2017-03-29.xlsx"
    oneday = datetime.timedelta(days=1)
    yesterday_time = datetime.datetime.now() - oneday
    
    filepath="D:\\ftpfiles\\"
    #filepath="E:\workspace\pythontest\import\\"
    fileList =[]
    f_list = os.listdir(filepath)
    # print f_list
    for i2 in f_list:
        # os.path.splitext():
        if os.path.splitext(i2)[1] == '.xlsx':
            fileList.append(i2)    

    excelfile=filepath+fileList[len(fileList)-1]
    print(excelfile)
    if os.path.exists(excelfile):
        message = 'OK, the fucking file exists. start data import'
        #dailyOrder=excelfile[13:23]
        dailyOrder=excelfile[len(excelfile)-15:len(excelfile)-5]
        
        data = xlrd.open_workbook(excelfile)
        
        titleList = getTitleSheet(excelfile)
        
        cfrList = getCfrSheet(excelfile)
        
        saleList = getSaleSheet(excelfile)
        
        foreCastList = getForeCastSheet(excelfile)
        
        #rm-bp1x9wm1s76107p35.mysql.rds.aliyuncs.com
        #aliroot
        #Arvato@2017
        config = {
              'host':'rm-bp16174sysuyxnj26.mysql.rds.aliyuncs.com',
              'port':3306,
              'user':'business',
              'password':'Arvato@0101',
              'db':'business',
              'charset':'utf8mb4',
              'cursorclass':pymysql.cursors.DictCursor,
              }
        # 创建连接
        connection = pymysql.connect(**config)
        
        try:
            clearData(connection,dailyOrder)
            
            num = importTitleSheet(connection,dailyOrder,titleList)
            
            importCfrSheet(connection,dailyOrder,cfrList)
            
            importSaleSheet(connection,dailyOrder,saleList)
            
            importForeCastSheet(connection,dailyOrder,foreCastList)
            
        finally:
            connection.close();
    else:
        message = 'Sorry, I cannot find the fucking file.'

    
    
        
    
    #logging.debug(result)
    #writeBackupResult("oasbackup.xls",result)
    #t.upload_small_file("C:\\software\\20161222.zip")
    #t.upload_big_file("C:\\software\\20161222-2.zip")
    #t.test_download_archive("769C75CDD0ACEA5A993E2D0B2A5457A9B5BE1F362B1A4ED1A2324A83BE741F41096C55C590D924B3C2BD11D075D4B3BB30650AE3287D6DA933D0E4D8C9759026")