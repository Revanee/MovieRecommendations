set MASTER_URL=spark://172.19.229.1:7077
set MAIN_JAR=.\target\scala-2.10\movierecommendations_2.10-0.1.jar
set FILE_SERVER_URL=http://172.19.229.1:8082/ml-latest-small
set DEPENDENCY_PACKAGES=com.databricks:spark-csv_2.10:1.4.0

spark-submit ^
--packages "%DEPENDENCY_PACKAGES%" ^
--master %MASTER_URL% ^
%MAIN_JAR%
