@echo off
cd /d %~dp0
call venv\Scripts\activate

python -m api.bergapi
python -m reglament_task.deliveryminprice
python -m accessold.dataforrepricing
python -m from_folder_to_df.create_files_to_access
echo bergapi completed.

rem Запись информации о выполнении в лог-файл
echo Процедура %~nx0 выполнена в %date% %time% >> log.txt

timeout /t 5 >nul
exit
