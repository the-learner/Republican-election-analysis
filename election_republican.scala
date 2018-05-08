import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import org.apache.spark.util.IntParam
import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel} 
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.evaluation.ClusteringEvaluator

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import sqlContext._
val schema = StructType(Array(
  StructField("state", StringType, true), 
  StructField("state_abbr", StringType, true), 
  StructField("county", StringType, true), 
  StructField("fips", StringType, true), 
  StructField("party", StringType, true), 
  StructField("candidate", StringType, true), 
  StructField ("votes", IntegerType, true), 
  StructField("fraction_votes", DoubleType, true)))
 
val df = spark.read.option("header","true").schema(schema).csv("/home/fenixwork/Downloads/temporary/mllib_answers/primary_results.csv")
val df_D = df.filter($"party" === "Republican")
df_D.createOrReplaceTempView("election")
val temp = spark.sql("SELECT * FROM election INNER JOIN (SELECT fips as b, MAX(fraction_votes) AS a FROM election GROUP BY fips) groupedtt WHERE election.fips = groupedtt.b AND election.fraction_votes = groupedtt.a")
temp.createOrReplaceTempView("election1")
val temp = spark.sql("SELECT state, state_abbr, county, fips, party, candidate, votes, fraction_votes FROM election1")
val d_winner = temp
d_winner.createOrReplaceTempView("republican")
val temp = spark.sql("select state, candidate, count(candidate) as countyswon from republican group by state, candidate")
val d_state = temp 
d_state.createOrReplaceTempView("state")
val schema1 = StructType(Array( 
	StructField("fips", StringType, true), 
	StructField("area_name", StringType, true), 
	StructField("state_abbreviation", StringType, true), 
	StructField("Population_2014", IntegerType, true), 
	StructField("Population_2010_Apr1", IntegerType, true), 
	StructField("Change_in_Population_percent", DoubleType, true), 
	StructField("Population_2010", IntegerType, true), 
	StructField("Persons_under_5", DoubleType, true), 
	StructField("Persons_under_18", DoubleType, true), 
	StructField("Persons_65_years_over", DoubleType, true), 
	StructField("Female_persons_percent", DoubleType, true), 
	StructField("White_alone", DoubleType, true), 
	StructField("Black_or_African_American_alone", DoubleType, true), 
	StructField("American_Indian_and_Alaska_Native_alone", DoubleType, true), 
	StructField("Asian_alone", DoubleType, true), 
	StructField("Native_Hawaiian_and_Other_Pacific_Islander_alone", DoubleType, true), 
	StructField("Two_or_More_Races", DoubleType, true), 
	StructField("Hispanic_or_Latino", DoubleType, true), 
	StructField("White_alone_not_Hispanic_or_Latino", DoubleType, true), 
	StructField("Living_in_same_house_1_year_&_over", DoubleType, true), 
	StructField("Foreign_born_persons", DoubleType, true), 
	StructField("Language_other_than_English_spoken_at_home", DoubleType, true), 
	StructField("High_school_graduate_or_higher", DoubleType, true), 
	StructField("Bachelor_degree_or_higher", DoubleType, true), 
	StructField("Veterans", IntegerType, true), 
	StructField("Mean_travel_time_to_work", DoubleType, true), 
	StructField("Housing_units", IntegerType, true), 
	StructField("Homeownership_rate", DoubleType, true), 
	StructField("Housing_units_in_multi_unit_structures", DoubleType, true), 
	StructField("Median_value_of_owner_occupied_housing_units", IntegerType, true), 
	StructField("Households", IntegerType, true), 
	StructField("Persons_per_household", DoubleType, true), 
	StructField("Per_capita_money_income", IntegerType, true), 
	StructField("Median_household_income", IntegerType, true), 
	StructField("Persons_below_poverty_level", DoubleType, true), 
	StructField("Private_nonfarm_establishments", IntegerType, true), 
	StructField("Private_nonfarm_employment", IntegerType, true), 
	StructField("Private_nonfarm_employment_percentage_change", DoubleType, true), 
	StructField("Nonemployer_establishments", IntegerType, true), 
	StructField("Total_number_of_firms", IntegerType, true), 
	StructField("Black_owned_firms", DoubleType, true), 
	StructField("American_Indian_and_Alaska_Native_owned_firms", DoubleType, true), 
	StructField("Asian_owned_firms", DoubleType, true), 
	StructField("Native_Hawaiian_and_Other_Pacific_Islander_owned_firms", DoubleType, true), 
	StructField("Hispanic_owned_firms", DoubleType, true), 
	StructField("Women_owned_firms", DoubleType, true), 
	StructField("Manufacturers_shipments", DoubleType, true), 
	StructField("Merchant_wholesaler_sales", DoubleType, true), 
	StructField("Retail_sales", DoubleType, true), 
	StructField("Retail_sales_per_capita", IntegerType, true), 
	StructField("Accommodation_and_food_services_sales", IntegerType, true), 
	StructField("Building_permits", IntegerType, true), 
	StructField("Land_area_in_square_miles", DoubleType, true), 
	StructField("Population_per_square_mile", DoubleType, true)))
val df1 = sqlContext.read.option("header","true").schema(schema1).csv("/home/fenixwork/Downloads/temporary/mllib_answers/county_facts.csv")
df1.createOrReplaceTempView("facts")
val temp = spark.sql("SELECT facts.fips as fips, republican.state as state, facts.state_abbreviation as state_abbreviation, area_name, candidate, Persons_65_years_over, Female_persons_percent, White_alone, Black_or_African_American_alone, Asian_alone, Hispanic_or_Latino, Foreign_born_persons, Language_other_than_English_spoken_at_home, Bachelor_degree_or_higher, Veterans, Homeownership_rate, Median_household_income, Persons_below_poverty_level, Population_per_square_mile FROM facts INNER JOIN republican ON CAST(facts.fips AS INT) = CAST(republican.fips AS INT)")
val df_facts = temp

val dt = df_facts.filter($"candidate" === "Donald Trump")
val bc = df_facts.filter($"candidate" === "Ben Carson")
val jk = df_facts.filter($"candidate" === "John Kasich")
val mr = df_facts.filter($"candidate" === "Marco Rubio")
val tc = df_facts.filter($"candidate" === "Ted Cruz")
val wdt = dt.withColumn("w_dt", lit(1)).withColumn("w_bc", lit(0)).withColumn("w_jk", lit(0)).withColumn("w_mr", lit(0)).withColumn("w_tc", lit(0))
val wbc = bc.withColumn("w_dt", lit(0)).withColumn("w_bc", lit(1)).withColumn("w_jk", lit(0)).withColumn("w_mr", lit(0)).withColumn("w_tc", lit(0))
val wjk = jk.withColumn("w_dt", lit(0)).withColumn("w_bc", lit(0)).withColumn("w_jk", lit(1)).withColumn("w_mr", lit(0)).withColumn("w_tc", lit(0))
val wmr = mr.withColumn("w_dt", lit(0)).withColumn("w_bc", lit(0)).withColumn("w_jk", lit(0)).withColumn("w_mr", lit(1)).withColumn("w_tc", lit(0))
val wtc = tc.withColumn("w_dt", lit(0)).withColumn("w_bc", lit(0)).withColumn("w_jk", lit(0)).withColumn("w_mr", lit(0)).withColumn("w_tc", lit(1))
wdt.createOrReplaceTempView("wdt") 
wbc.createOrReplaceTempView("wbc") 
wjk.createOrReplaceTempView("wjk") 
wmr.createOrReplaceTempView("wmr") 
wtc.createOrReplaceTempView("wtc") 

val result = spark.sql("SELECT * FROM wdt UNION ALL SELECT * FROM wbc UNION ALL SELECT * FROM wjk UNION ALL SELECT * FROM wmr UNION ALL SELECT * FROM wtc")
result.createOrReplaceTempView("result")

val featureCols = Array("Persons_65_years_over", "Female_persons_percent", "White_alone","Black_or_African_American_alone", "Asian_alone", "Hispanic_or_Latino", "Foreign_born_persons", "Language_other_than_English_spoken_at_home", "Bachelor_degree_or_higher", "Veterans", "Homeownership_rate", "Median_household_income", "Persons_below_poverty_level", "Population_per_square_mile", "w_dt", "w_bc","w_jk","w_mr","w_tc")

val rows = new VectorAssembler().setInputCols(featureCols).setOutputCol("features").transform(result)
val kmeans = new org.apache.spark.ml.clustering.KMeans().setK(4).setFeaturesCol("features").setPredictionCol("prediction")
val model = kmeans.fit(rows) 
println("Cluster centres : ")
model.clusterCenters.foreach(println)

val categories = model.transform(rows) 
val evaluator = new ClusteringEvaluator()

categories.createOrReplaceTempView("c")

/*spark.sql("select SUM(Persons_65_years_over) ,SUM(Female_persons_percent) ,SUM(White_alone),SUM(Black_or_African_American_alone) ,SUM(Asian_alone) ,SUM(Hispanic_or_Latino) ,SUM(Foreign_born_persons) ,SUM(Language_other_than_English_spoken_at_home) ,SUM(Bachelor_degree_or_higher) ,SUM(Veterans) ,SUM(Homeownership_rate) ,SUM(Median_household_income) ,SUM(Persons_below_poverty_level) ,SUM(Population_per_square_mile) from c group_by w_dt=1 ").show
//spark.sql("select Persons_65_years_over from c group by prediction").show
spark.sql("select prediction, count(*), sum(w_dt), sum(w_bc),sum(w_jk),sum(w_mr),sum(w_tc) from c group by prediction").show
spark.sql("select state, count(*), sum(w_dt), sum(w_bc) from c where prediction=1 group by state").show
spark.sql("select state, count(*), sum(w_dt), sum(w_bc),sum(w_jk),sum(w_mr),sum(w_tc) from c where prediction=1 group by state").show
spark.sql("select * from c where w_dt=1").show*/

println("Top 3 states : ")
spark.sql("select state from c group by state ORDER BY mean(w_dt) DESC,sum(w_dt) DESC LIMIT 3").show
println("Weak 3 states : ")
spark.sql("select state from c group by state ORDER BY mean(w_dt),sum(w_dt) LIMIT 3").show

//spark.sql("select SUM(Persons_65_years_over) ,SUM(Female_persons_percent) ,SUM(White_alone),SUM(Black_or_African_American_alone) ,SUM(Asian_alone) ,SUM(Hispanic_or_Latino) ,SUM(Foreign_born_persons) ,SUM(Language_other_than_English_spoken_at_home) ,SUM(Bachelor_degree_or_higher) ,SUM(Veterans) ,SUM(Homeownership_rate) ,SUM(Median_household_income) ,SUM(Persons_below_poverty_level) ,SUM(Population_per_square_mile) from c where w_dt=1 MINUS select SUM(Persons_65_years_over) ,SUM(Female_persons_percent) ,SUM(White_alone),SUM(Black_or_African_American_alone) ,SUM(Asian_alone) ,SUM(Hispanic_or_Latino) ,SUM(Foreign_born_persons) ,SUM(Language_other_than_English_spoken_at_home) ,SUM(Bachelor_degree_or_higher) ,SUM(Veterans) ,SUM(Homeownership_rate) ,SUM(Median_household_income) ,SUM(Persons_below_poverty_level) ,SUM(Population_per_square_mile) from c where w_bc=1").show
