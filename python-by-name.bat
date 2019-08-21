set PYTHON_HOME=%C:\Anaconda3
set PYTHON_NAME=%1.exe

copy "%PYTHON_HOME%\python.exe" "%PYTHON_HOME%\%PYTHON_NAME%"
set PYTHON_PATH="%PYTHON_HOME%\%PYTHON_NAME%"
set args=%*
set args=%args:* =%
"%PYTHON_HOME%\%PYTHON_NAME%" %args%