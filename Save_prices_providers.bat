@echo off
cd /d %~dp0
call venv\Scripts\activate
python -m from_folder_to_df.outlook_utils
echo Save prices in Output.
python -m from_folder_to_df.prices_to_sql
echo Prices downloaded to psql.
pause
