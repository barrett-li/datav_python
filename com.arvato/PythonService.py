#-*- coding:utf-8 -*-
import win32serviceutil 
import win32service 
import win32event 
from auto_lunbo import AudiDataVLunBo

class PythonService(win32serviceutil.ServiceFramework): 
    #服务名
    _svc_name_ = "Audi DataV ADC集客画像"
    #服务显示名称
    _svc_display_name_ = "Audi DataV ADC集客画像 测试环境"
    #服务描述
    _svc_description_ = "ADC集客画像，每隔300秒更换一个 "

    def __init__(self, args): 
        win32serviceutil.ServiceFramework.__init__(self, args) 
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.logger = self._getLogger()
        self.isAlive = True
        
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

    def SvcDoRun(self):
        import time
        AudiDataVLunBo().lunbo()
        while self.isAlive:
            self.logger.error("I am alive.")
            time.sleep(5)
        # 等待服务被停止 
        #win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE) 
            
    def SvcStop(self): 
        # 先告诉SCM停止这个过程 
        self.logger.error("svc do stop....")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING) 
        # 设置事件 
        win32event.SetEvent(self.hWaitStop) 
        self.isAlive = False

if __name__=='__main__': 
    win32serviceutil.HandleCommandLine(PythonService)