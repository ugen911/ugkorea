@echo off
cd /d %~dp0
call venv\Scripts\activate

python -m parsers.parser_autocoreec
echo parser_autocoreec completed.

rem Запись информации о выполнении в лог-файл
echo Процедура %~nx0 выполнена в %date% %time% >> log.txt

timeout /t 5 >nul
exit
