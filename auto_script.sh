bash install_spark.sh
pip install dvc


dvc exp run -f web_scraping_checks
dvc exp run -f data_transformation_checks
dvc exp run -f machine_learning_checks
dvc exp run -f data_transformation_checks
dvc exp run -f write_into_database_checks

dvc exp show