# practice1c

```$sh
hdfs dfs -mkdir -p hdfs://localhost:9000/dgd2/sparksql/in/csv/dim1
hdfs dfs -mkdir -p hdfs://localhost:9000/dgd2/sparksql/in/csv/dim2
hdfs dfs -mkdir -p hdfs://localhost:9000/dgd2/sparksql/in/json/dim
hdfs dfs -mkdir -p hdfs://localhost:9000/dgd2/sparksql/in/orc/dim
hdfs dfs -mkdir -p hdfs://localhost:9000/dgd2/sparksql/in/parquet/dim

hdfs dfs -copyFromLocal -f $(pwd)/sparksql/d_countries_1.csv hdfs://localhost:9000/dgd2/sparksql/in/csv/dim1/
hdfs dfs -copyFromLocal -f $(pwd)/sparksql/d_countries_2.csv hdfs://localhost:9000/dgd2/sparksql/in/csv/dim1/
hdfs dfs -copyFromLocal -f $(pwd)/sparksql/d_countries_3.csv hdfs://localhost:9000/dgd2/sparksql/in/csv/dim2/
hdfs dfs -copyFromLocal -f $(pwd)/sparksql/d_countries_4.json hdfs://localhost:9000/dgd2/sparksql/in/json/dim/
hdfs dfs -copyFromLocal -f $(pwd)/sparksql/d_countries_5.snappy.orc hdfs://localhost:9000/dgd2/sparksql/in/orc/dim/
hdfs dfs -copyFromLocal -f $(pwd)/sparksql/d_countries_6.snappy.parquet hdfs://localhost:9000/dgd2/sparksql/in/parquet/dim/

```
