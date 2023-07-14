pip install -r requirements.txt
allure generate --clean
pytest --alluredir=allure-report/
allure serve allure-report/