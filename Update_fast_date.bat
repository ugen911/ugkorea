@echo off
cd /d %~dp0
call venv\Scripts\activate

python -m reglament_task.update_price_stock_old
echo From old prices update.

pause
