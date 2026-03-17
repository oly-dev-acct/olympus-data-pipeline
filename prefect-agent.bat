@echo off

rem Step 1: Change directory to the desired location
cd C:\Users\revenuemanagement\Documents\Olympus-Property-Analytics\Olympus-Property-Analytics-master

rem Step 2: Activate the virtual environment
call venv\Scripts\activate.bat

rem Step 3: Start the prefect agent
start prefect agent start -p olympus-analytics-workpool
