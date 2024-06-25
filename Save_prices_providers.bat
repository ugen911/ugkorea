@echo off
cd /d %~dp0
call venv\Scripts\activate
python -m from_folder_to_df.outlook_utils
echo Save prices in Output.
python -m from_folder_to_df.prices_to_sql
echo Prices downloaded to psql.
python -m assessold.create_files_to_access
echo files for access is ready.
pause
