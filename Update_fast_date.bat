@echo off
cd /d %~dp0
call venv\Scripts\activate

python -m reglament_task.update_price_stock_old
echo From old prices update.

rem Запись информации о выполнении в лог-файл
echo Процедура %~nx0 выполнена в %date% %time% >> log.txt

timeout /t 10 >nul
exit
