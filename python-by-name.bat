set PYTHON_HOME=%~dp0\venv\Scripts
set PYTHON_NAME=%1.exe
set AUTOMATOR_HOME=%C:\Automator_3_v1.0
copy "%PYTHON_HOME%\python.exe" "%AUTOMATOR_HOME%\%PYTHON_NAME%"
set args=%*
set args=%args:* =%
"%AUTOMATOR_HOME%\%PYTHON_NAME%" %args%