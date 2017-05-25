import random
import logging
import xlrd
import time, datetime
import xlutils.copy
import time

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

class AudiDataVLunBo(): 
    
    def __init__(self): 
        self.logger = self._getLogger()
        
    def _getLogger(self):
        import logging
        import os
        import inspect
        
        logger = logging.getLogger('[AudiDataV]')
        
        this_file = inspect.getfile(inspect.currentframe())
        dirpath = os.path.abspath(os.path.dirname(this_file))
        handler = logging.FileHandler(os.path.join(dirpath, "service.log"))
        
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        return logger
    
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
        
    
    
    def getList(self,connection,dailyOrder,activeNum):
        # 执行sql语句
        
        with connection.cursor() as cursor:
            #for rownum in range(0,len(titleList)):
                # 执行sql语句，插入记录
    
            getCfr ="SELECT * FROM datav_adc_user where dailyorder = %s"
            result = cursor.execute(getCfr, (dailyOrder));
            adcUserList = cursor.fetchmany(result)
            
            if activeNum < adcUserList[0]['id']:
                activeNum = adcUserList[0]['id']
            else:    
                if activeNum > adcUserList[len(adcUserList)-1]['id']:
                    activeNum = adcUserList[0]['id']
                
            print(activeNum)
            
                
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
        return activeNum
    
    def setActvie(self,connection,dailyOrder,activeNum):
        # 执行sql语句
        
        with connection.cursor() as cursor:
            #for rownum in range(0,len(titleList)):
                # 执行sql语句，插入记录
            setZero ="update datav_adc_user set active = 0"
            cursor.execute(setZero );
            
            setActive ="update datav_adc_user set active = 1 where dailyorder = %s and id = %s"
            effectNum = cursor.execute(setActive, (dailyOrder,activeNum));
            print("干了 ： "+str(effectNum))
            if effectNum == 0:
                activeNum = self.getList(connection,dailyOrder,1)
                activeNum = self.setActvie(connection,dailyOrder,activeNum)
                
        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
        activeNum = activeNum+1
        return activeNum
    
    def lunbo(self):
        activeNum=1
        
        while 1:
            self.logger.error("轮播开始：查询一次数据库记录(每小时查一次)")
            #excelfile="D:\\ftpfiles\\datav_import_2017-03-29.xlsx"
            oneday = datetime.timedelta(days=1)
            yesterday_time = datetime.datetime.now() - oneday
        
            #filepath="D:\\ftpfiles\\"
            filepath="E:\workspace\datav_python\import\\"
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
                #data = xlrd.open_workbook(excelfile)
                
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
                    activeNum = self.getList(connection,dailyOrder,activeNum)
                    
                    fuckNum = 0
                    while 1:
                        self.logger.error("轮播开始：每fuck 720次就重新取一次数据；第"+str(fuckNum)+"次")
                        activeNum = self.setActvie(connection,dailyOrder,activeNum)
                        fuckNum+=1
                        time.sleep(5)
                        if fuckNum > 720 :
                            self.logger.error("轮播开始：干够了720次，撤")
                            break
                    
                    
                finally:
                    connection.close();
            else:
                message = 'Sorry, I cannot find the fucking file.'

            #延时300秒，然后再fuck
            #time.sleep(5)
            
if __name__ == '__main__':

    AudiDataVLunBo().lunbo()
    

    
    
        
    
    #logging.debug(result)
    #writeBackupResult("oasbackup.xls",result)
    #t.upload_small_file("C:\\software\\20161222.zip")
    #t.upload_big_file("C:\\software\\20161222-2.zip")
    #t.test_download_archive("769C75CDD0ACEA5A993E2D0B2A5457A9B5BE1F362B1A4ED1A2324A83BE741F41096C55C590D924B3C2BD11D075D4B3BB30650AE3287D6DA933D0E4D8C9759026")