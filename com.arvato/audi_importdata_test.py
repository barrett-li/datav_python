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
    #pythondate  = date - datetime.timedelta(days = 1462)
    pythondate  = date - datetime.timedelta(days = 1462)
    #return pythondate.strftime("%G-%h")%F %T
    return pythondate.strftime("%Y/%m/%d %T")

def getdate2(date):
    #if isinstance(date, float):
    #    date = int(date)
    #d = datetime.date.fromordinal(__s_date + date)
    #pythondate  = date - datetime.timedelta(days = 1462)
    pythondate  = date - datetime.timedelta(days = 1462)
    return pythondate.strftime("%D")
    

def getMainSheet(excelfile):

    #主区域
    table = data.sheet_by_index(1)
    nrows = table.nrows
    ncols = table.ncols
    colnames =  table.row_values(0)
    
    #print(table.cell(1,2).value)
    mainList = []
    app = {}
    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        print(row)
        
        if rownum < 8:
            if rownum == 2:
                d = xlrd.xldate.xldate_as_datetime(row[2], 1)
                app['test'] = getdate2(d)
            else:
                app[row[1]] = str(round(float(row[2])))
        else:    
            #fuck = float('%.2f'%(row[2]*100)) 
            #timeprocess = str(int(fuck))+'%'
            #app[row[1]] = str(int(fuck))+'%' 
            app[row[1]] = row[2]
        
        
        #cfrTup=(row[0],row[1],row[2])
        print(app)
    mainList.append(app)
    
    return mainList

def getSaleOrderSheet(excelfile):
    
    #本月ADC业务成交量排名
    table = data.sheet_by_index(2)
    nrows = table.nrows
    ncols = table.ncols
    colnames =  table.row_values(0)
    
    saleOrderList =[]
    
    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        
        castTup=(row[0],row[1],row[2])
        #for i in range(len(colnames)):
        #    app[colnames[i]] = str(row[i]) 

        saleOrderList.append(castTup)

    return saleOrderList


def getAdcStructureSheet(excelfile):
    
    #ADC成交结构比
    table = data.sheet_by_index(3)
    nrows = table.nrows
    ncols = table.ncols
    colnames =  table.row_values(0)
    adcStructureList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        castTup=(row[0],row[1])
        #for i in range(len(colnames)):
        #    app[colnames[i]] = str(row[i]) 

        adcStructureList.append(castTup)
        

    return adcStructureList


def getSaleTrendSheet(excelfile):
    
    table = data.sheet_by_index(4)
    nrows = table.nrows
    ncols = table.ncols
    
    saleTrendList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        #d = xlrd.xldate.xldate_as_datetime(table.cell(rownum,0).value, 1).strftime('%Y-%m-%d') 
        d = xlrd.xldate.xldate_as_datetime(table.cell(rownum,0).value, 1)
        castTup=(getdate(d),row[1])
        print(castTup)
        saleTrendList.append(castTup)

    return saleTrendList

def getUsefulSheet(excelfile):
    
    table = data.sheet_by_index(5)
    nrows = table.nrows
    ncols = table.ncols
    
    usefulList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        castTup=(row[0],row[1])
        print(castTup)
        usefulList.append(castTup)

    return usefulList

def getCarSaleSheet(excelfile):
    
    table = data.sheet_by_index(7)
    nrows = table.nrows
    ncols = table.ncols
    
    carSaleList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        castTup=(row[0],row[1],row[2])
        print(castTup)
        carSaleList.append(castTup)

    return carSaleList

def getMessageSheet(excelfile):
    
    table = data.sheet_by_index(8)
    nrows = table.nrows
    ncols = table.ncols
    
    messageList =[]

    for rownum in range(1,nrows):
        row = table.row_values(rownum)
        castTup=(row[0],row[1])
        print(castTup)
        messageList.append(castTup)

    return messageList


def clearData(connection,dailyOrder):
    
    
    
    # 执行sql语句
    
    with connection.cursor() as cursor:
        #for rownum in range(0,len(titleList)):
            # 执行sql语句，插入记录

        sql = 'delete from datav_main where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_sales_order where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_sales_adc_structure where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_sales_adc_region_order where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_sales_trend where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_useful_user where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_car_saled where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
        sql = 'delete from datav_real_message where dailyorder = %s'
        cursor.execute(sql, (dailyOrder))
        
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()


def importMainSheet(connection,dailyOrder,mainList):
    
    
    # 执行sql语句
    with connection.cursor() as cursor:
        #for rownum in range(0,len(titleList)):
            # 执行sql语句，插入记录
        sql = '''INSERT INTO datav_main (dailyorder,adcsales,date2,
                    totalsales,expectsales,totalleads,
                    activeleads,tostoreleads,avtiverate,
                    tostorerate,conversionrate) VALUES (%s, %s,%s, 
                    %s,%s, %s,
                    %s, %s,%s, 
                    %s,%s)'''
        #fuck = float('%.2f'%(titleList[4]*100)) 
        #timeprocess = str(int(fuck))+'%'

        print(mainList)
        fuck  = mainList[0].get('totalsales')
        #round(float(mainList[0]['adcsales']))   
        num  = cursor.execute(sql, (dailyOrder, mainList[0].get('adcsales'),mainList[0].get('test'),
                                    mainList[0].get('totalsales'),mainList[0].get('expectsales'),mainList[0].get('totalleads'),
                                    mainList[0].get('activeleads'),mainList[0].get('tostoreleads'),mainList[0].get('avtiverate'),
                                    mainList[0].get('tostorerate'),mainList[0].get('conversionrate')));
                           
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()
     
    return num 

def importSaleOrderSheet(connection,dailyOrder,saleOrderList):
    
    # 执行sql语句
    with connection.cursor() as cursor:
        for rownum in range(0,len(saleOrderList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(saleOrderList[rownum])
            
            sql = 'INSERT INTO datav_sales_order (dailyorder,dealer,area,monthadcsales) VALUES (%s, %s,%s, %s)'
            #daily = str(cfrList[rownum][1]*100)[0:5]+'%'
            #mtd = str(cfrList[rownum][2]*100)[0:5]+'%'
            num  = cursor.execute(sql, (dailyOrder,saleOrderList[rownum][0] ,saleOrderList[rownum][1],saleOrderList[rownum][2]))
            
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()
     
    return num 


def importAdcStructureSheet(connection,dailyOrder,adcStructure):
    
    # 执行sql语句
    with connection.cursor() as cursor:
        
        #region
        adcsales=''
        totalsales=''
        
            # 执行sql语句，插入记录cfrList[rownum]
        print(adcStructure[0][1])
            
        sql = '''INSERT INTO datav_sales_adc_structure (dailyorder,adcsales,totalsales) VALUES (%s, %s,%s)'''
                   
        cursor.execute(sql, (dailyOrder,adcStructure[0][1],adcStructure[1][1]) )
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        
        
        #category
        for rownum in range(3,len(adcStructure)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(adcStructure[rownum])
            sql = 'INSERT INTO datav_sales_adc_region_order (dailyorder,area,adcsales) VALUES (%s, %s,%s)'
            cursor.execute(sql, (dailyOrder, adcStructure[rownum][0],adcStructure[rownum][1]))


    connection.commit()
     
    return num 


def importSaleTrendSheet(connection,dailyOrder,saleTrendList):
    
    # 执行sql语句
    with connection.cursor() as cursor:
        for rownum in range(0,len(saleTrendList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(saleTrendList[rownum])
            sql = 'INSERT INTO datav_sales_trend (dailyorder,month,monthadcsales) VALUES (%s, %s,%s)'
            num  = cursor.execute(sql, (dailyOrder, saleTrendList[rownum][0],saleTrendList[rownum][1]))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
     
    return num 

def importUsefulSheet(connection,dailyOrder,usefulList):
    
    with connection.cursor() as cursor:
        for rownum in range(0,len(usefulList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(usefulList[rownum])
            sql = 'INSERT INTO datav_useful_user (dailyorder,channel,activeleads) VALUES (%s, %s,%s)'
            num  = cursor.execute(sql, (dailyOrder, usefulList[rownum][0],usefulList[rownum][1]))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
     
    return num 


def importCarSaleSheet(connection,dailyOrder,carSaleList):
    
    with connection.cursor() as cursor:
        for rownum in range(0,len(carSaleList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(carSaleList[rownum])
            sql = 'INSERT INTO datav_car_saled (dailyorder,vehicle,activeleads,adcsales) VALUES (%s, %s,%s,%s)'
            num  = cursor.execute(sql, (dailyOrder, carSaleList[rownum][0],carSaleList[rownum][1],carSaleList[rownum][2]))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
     
    return num 


def importMessageSheet(connection,dailyOrder,messageList):
    with connection.cursor() as cursor:
        for rownum in range(0,len(messageList)):
            # 执行sql语句，插入记录cfrList[rownum]
            print(messageList[rownum])
            sql = 'INSERT INTO datav_real_message (dailyorder,statement) VALUES (%s, %s)'
            num  = cursor.execute(sql, (dailyOrder, messageList[rownum][1]))
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
     
    return num

if __name__ == '__main__':

    #excelfile="D:\\ftpfiles\\datav_import_2017-03-29.xlsx"
    oneday = datetime.timedelta(days=1)
    yesterday_time = datetime.datetime.now() - oneday
    
    #filepath="D:\\ftpfiles\\"
    filepath="E:\workspace\AudiDataV\import\\"
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
        print(dailyOrder)
        data = xlrd.open_workbook(excelfile)
        
        mainList = getMainSheet(excelfile)
        print(mainList)
        
        saleOrderList = getSaleOrderSheet(excelfile)
        print(saleOrderList)
        
        #ADC成交结构比
        adcStructure = getAdcStructureSheet(excelfile)
        print(adcStructure)
        
        #成交趋势
        saleTrendList = getSaleTrendSheet(excelfile)
        
        #本月有效集客占比
        usefulList = getUsefulSheet(excelfile)
        print(usefulList)
        
        #ADC集客画像
        
        #各车型成交量统计
        carSaleList = getCarSaleSheet(excelfile)
        print(carSaleList)
        
        #实时消息
        messageList = getMessageSheet(excelfile)
        print(messageList)
        
        #test
        #rm-bp15700osx362i054.mysql.rds.aliyuncs.com
        #aliroot
        #Arvato@0101
        config = {
              'host':'rm-bp15700osx362i054.mysql.rds.aliyuncs.com',
              'port':3306,
              'user':'aliroot',
              'password':'Arvato@0101',
              'db':'test',
              'charset':'utf8mb4',
              'cursorclass':pymysql.cursors.DictCursor,
              }
        # 创建连接
        connection = pymysql.connect(**config)
        
        try:
            clearData(connection,dailyOrder)
            
            num = importMainSheet(connection,dailyOrder,mainList)
            print(num)
            importSaleOrderSheet(connection,dailyOrder,saleOrderList)
            
            importAdcStructureSheet(connection,dailyOrder,adcStructure)
            
            importSaleTrendSheet(connection,dailyOrder,saleTrendList)
            
            importUsefulSheet(connection,dailyOrder,usefulList)
            
            importCarSaleSheet(connection,dailyOrder,carSaleList)
            
            importMessageSheet(connection,dailyOrder,messageList)
            
        finally:
            connection.close();
    else:
        message = 'Sorry, I cannot find the fucking file.'

    
    
        
    
    #logging.debug(result)
    #writeBackupResult("oasbackup.xls",result)
    #t.upload_small_file("C:\\software\\20161222.zip")
    #t.upload_big_file("C:\\software\\20161222-2.zip")
    #t.test_download_archive("769C75CDD0ACEA5A993E2D0B2A5457A9B5BE1F362B1A4ED1A2324A83BE741F41096C55C590D924B3C2BD11D075D4B3BB30650AE3287D6DA933D0E4D8C9759026")