@echo off
cd /d %~dp0
call venv\Scripts\activate
python -m reglament_task.update_analitics_gl
echo Updated all date from analitic 1c.
python -m reglament_task.update_price_stock_old
echo From old prices update.
pause
