from BI.utilities import UrlMonitor


if __name__ == '__main__':
    restart_luna_bat = 'C:\\Luna_Production\\restart_server.bat'
    restart_luna_dev_bat = 'C:\\Luna_Dev\\dev_server_restart.bat'
    luna_production = UrlMonitor('Luna_Production_Server', 'http://luna.vivintsolar.com', restart_luna_bat)
    luna_dev = UrlMonitor('Luna_Dev_Server', 'http://luna.vivintsolar.com:8000', restart_luna_dev_bat)
    luna_production.run()
    luna_dev.run()
