# chart_finplot
Для построения графиков FinPlot. Набор старых пакетов.  

Создать среду виртуального окружения Python 3.9.13 (более поздние не работают).  
Установить следующие пакеты:  
`pip install finplot==1.8.0`  
При этом будут установлены последние версии пакетов с зависимостями, 
поэтому будет необходимо переустановить их на более ранние версии.  
```commandline
pip install pandas==1.2.3
pip install pyqtgraph==0.12.0
pip install PyQt5==5.15.2
pip install numpy==1.20.2
pip install python-dateutil==2.8.1
pip install pytz==2020.4
pip install six==1.15.0
pip install PyQt5-sip==12.8.1
```  
Установку нужных пакетов можно осуществить [requirements.txt](requirements.txt)  
`pip install -r requirements.txt`  

Пример полученного графика:  
![img.png](img.png)


