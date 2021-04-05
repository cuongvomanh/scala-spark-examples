# practice2j

```$sh
hdfs dfs -mkdir -p hdfs://localhost:9000/dgd2/sparksql/in/covid19/parquet/dim/
hdfs dfs -copyFromLocal -f $(pwd)/sparksql/f_covid19_202010.csv hdfs://localhost:9000/dgd2/sparksql/in/covid19/parquet/dim/
```
