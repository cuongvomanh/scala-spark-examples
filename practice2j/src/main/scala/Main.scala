import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //  import spark.sqlContext.implicits._
  // Ex1
  val countryPath = hdfsPath + "/dgd2/sparksql/in/parquet/dim"
  val rawCountry = spark.read.parquet(countryPath)
  val country = rawCountry.filter('iso_code.isNotNull && 'iso_code =!= "OWID_WRL")

  //iso_code, date, total_cases, new_cases, total_deaths, new_deaths, icu_patients, hosp_patients, total_tests, new_tests
  val covidPath = hdfsPath + "/dgd2/sparksql/in/covid19/parquet/dim/"
  val rawCovid = spark.read.option("header", true).option("inferSchema", true).csv(covidPath)
  val covid = rawCovid.filter('iso_code.isNotNull && 'iso_code =!= "OWID_WRL")
      .withColumn("date", to_date('date, "yyyy-MM-dd"))
  covid.printSchema()
    country.createOrReplaceTempView("country")
    covid.createOrReplaceTempView("covid")
//  covid.show()
  //Ex1.1
//  val covidAgg = covid.filter('date <= to_timestamp(lit("30/10/2020"), "dd/MM/yyyy")).join(country, "iso_code")
//    .withColumn("total_cases_per_million", 'total_cases/'population*1000000)
//    .withColumn("new_cases_per_million", 'new_cases/'population*1000000)
//    .withColumn("total_deaths_per_million", 'total_deaths/'population*1000000)
//    .withColumn("new_deaths_per_million", 'new_deaths/'population*1000000)
//    .withColumn("total_tests_per_thousand", 'total_tests/'population*1000)
//    .withColumn("new_tests_per_thousand", 'new_tests/'population*1000)
//    .withColumn("positive_rate", 'total_cases/'total_tests)
////    .filter('location === "Vietnam")
////    .filter('location === "United States")
//    .sort('date.desc)
//    .select( 'iso_code, 'date, 'total_cases, 'new_cases, 'total_deaths, 'new_deaths, 'icu_patients, 'hosp_patients, 'total_tests, 'new_tests,
//      'total_cases_per_million, 'new_cases_per_million, 'total_deaths_per_million,
//      'new_deaths_per_million, 'total_tests_per_thousand,'new_tests_per_thousand, 'positive_rate)
//  val covidAggPath = hdfsPath + "/dgd2/sparksql/out/2j/parquet/f_covid_202010"
//  covidAgg.write.mode("overwrite").parquet(covidAggPath)
//  spark.read.parquet(covidAggPath).show()

  //Ex1.2
//  println(spark.table("country").count())
//  spark.sql("SELECT location, covid.iso_code, date, total_cases, new_cases, total_deaths, new_deaths, icu_patients, hosp_patients, total_tests, new_tests" +
//    ", total_cases/population*1000000 as total_cases_per_million" +
//    ", new_cases/population*1000000 as new_cases_per_million" +
//    ", total_deaths/population*1000000 as total_deaths_per_million" +
//    ", new_deaths/population*1000000 as new_deaths_per_million" +
//    ", total_tests/population*1000 as total_tests_per_thousand" +
//    ", new_tests/population*1000 as new_tests_per_thousand" +
//    ", total_cases/total_tests as positive_rate" +
//    " from covid join country on covid.iso_code == country.iso_code " +
////    " where location == 'Vietnam'" +
//    " order by date desc"
//  ).show()
  //Ex2
  //Ex2.1
//  val testRateContinent = covid.filter('date < to_date(lit("31/10/2020"), "dd/MM/yyyy"))
//    .join(country, "iso_code")
//    .groupBy('continent)
//    .agg(sum('new_tests) as "total_test", sum('population) as "continent_population")
//    .select('continent, 'total_test, 'total_test/'continent_population as "test_rate")
//    .sort('test_rate.desc)
//  val testRateContinentPath = hdfsPath + "/dgd2/sparksql/out/2j/parquet/test_rate"
//  testRateContinent.write.mode("overwrite").parquet(testRateContinentPath)
//  spark.read.parquet(testRateContinentPath).show()
  //Ex2.2
//  spark.sql("SELECT continent, sum(new_tests) as total_test, sum(new_tests)/sum(population) as test_rate " +
//    "from covid join country on covid.iso_code = country.iso_code " +
//    "where date <= '2020-10-30'" +
//    "group by continent " +
//    "order by test_rate desc").show()
  //Ex3
  //Ex3.1
//  val deadRateContinent = covid.filter('date <= to_timestamp(lit("30/10/2020"), "dd/MM/yyyy"))
//    .groupBy('iso_code).agg(sum('new_deaths) as "total_deaths_recal", sum('new_cases) as "total_cases_recal")
//    .join(country, "iso_code")
//    .select('iso_code, 'location, 'total_deaths_recal/'total_cases_recal as "death_rate")
//    .sort('death_rate.desc).limit(20)
//  val deadRateContinentPath = hdfsPath + "/dgd2/sparksql/out/2j/parquet/top20_deaths"
//  deadRateContinent.write.mode("overwrite").parquet(deadRateContinentPath)
//  spark.read.parquet(deadRateContinentPath).show()
  //Ex3.2
//  spark.sql("SELECT covid.iso_code, location, sum(new_deaths)/sum(new_cases) as death_rate " +
//    "from covid join country on covid.iso_code = country.iso_code " +
//    "where date <= '2020-10-30'" +
//    "group by covid.iso_code, location " +
//    "order by death_rate desc").show()
  //Ex4
  //Ex4.1
//  val newCaseByMonth = covid.filter('date >= to_date(lit("2020-01-01"), "yyyy-MM-dd") and 'date < to_date(lit("2020-11-01"), "yyyy-MM-dd"))
//    .groupBy('iso_code, date_format('date, "yyyyMM") as "month")
//    .agg(sum('new_cases) as "total_new_cases")
//    .join(country, "iso_code")
////    .filter('location === "Vietnam")
//    .select('iso_code, 'location, 'month, 'total_new_cases)
//  val newCaseByMonthPath = hdfsPath + "/dgd2/sparksql/out/2j/csv/new_case_by_month"
//  newCaseByMonth.write.option("header", true).option("sep", "|").mode("overwrite").csv(newCaseByMonthPath)
//  spark.read.option("header", true).option("sep", "|").csv(newCaseByMonthPath).show()
  //Ex4.2
  spark.sql("SELECT covid.iso_code, location , date_format(date, 'yyyyMM'), sum(new_cases) as total_new_cases " +
    "from covid join country on covid.iso_code = country.iso_code where date >= '2020-01-01' and date < '2020-10-31' " +
    "group by covid.iso_code, location, date_format(date, 'yyyyMM')").show()
}
