
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, datediff, initcap, sum, to_date, when}

object practice_set{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


//    val employees = List(
//      ("karthik", "2024-11-01"),
//      ("neha", "2024-10-20"),
//      ("priya", "2024-10-28"),
//      ("mohan", "2024-11-02"),
//      ("ajay", "2024-09-15"),
//      ("vijay", "2024-10-30"),
//      ("veer", "2024-10-25"),
//      ("aatish", "2024-10-10"),
//      ("animesh", "2024-10-15"),
//      ("nishad", "2024-11-01"),
//      ("varun", "2024-10-05"),
//      ("aadil", "2024-09-30")
//    ).toDF("name", "last_checkin")
//
//    val resultDF = employees
//      .withColumn("last_checkin", to_date(col("last_checkin"), "yyyy-MM-dd"))
//      .withColumn("name", initcap(col("name")))
//      .withColumn("status", when(datediff(current_date(), col("last_checkin")) <= 7, "Active" ).otherwise("Inactive"))
//
//    resultDF.show()
//
//    employees.createOrReplaceTempView("employees_sql")
//    val sql_result = spark.sql(
//      """
//        |select initcap(name) as name,
//        |case when datediff(current_date() - to_date(last_checkin, 'yyyy-MM-dd')) <= 7 then 'Active'
//        |else 'Inactive'
//        |end as status
//        |from employee_sql
//        |""".stripMargin)
//    sql_result.show()
//
//
//    ///////////question-2/////////////
//
//
//    val sales = List(
//      ("karthik", 60000),
//      ("neha", 48000),
//      ("priya", 30000),
//      ("mohan", 24000),
//      ("ajay", 52000),
//      ("vijay", 45000),
//      ("veer", 70000),
//      ("aatish", 23000),
//      ("animesh", 15000),
//      ("nishad", 8000),
//      ("varun", 29000),
//      ("aadil", 32000)
//
//    ).toDF("name", "total_sales")
//
//    val salesDF = sales
//      .withColumn("name", initcap(col("name")))
//      .withColumn("performance_status", when(col("total_sales") > 50000, "Excellent")
//        .when(col("total_sales")>25000 && col("total_sales")<50000, "Good").otherwise("Needs Improvement"))
//    val aggSalesdf = salesDF.groupBy(col("performance_status")).agg(sum(col("total_sales"))).alias("agg_sales")
//    aggSalesdf.show()

    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
      ("neha", "ProjectC", 80),
      ("neha", "ProjectD", 30),
      ("priya", "ProjectE", 110),
      ("mohan", "ProjectF", 40),
      ("ajay", "ProjectG", 70),
      ("vijay", "ProjectH", 150),
      ("veer", "ProjectI", 190),
      ("aatish", "ProjectJ", 60),
      ("animesh", "ProjectK", 95),
      ("nishad", "ProjectL", 210),
      ("varun", "ProjectM", 50),
      ("aadil", "ProjectN", 90)
      ).toDF("name", "project", "hours")

    val workloadDF = workload.withColumn("name",initcap(col("name")))
      .groupBy(col("name")).agg(sum("hours").alias("total_hrs"))
    val newWorkLoadDF = workloadDF.withColumn("workload", when(col("total_hrs")>200,"Overload").when(col("total_hrs")>100 && col("total_hrs") < 100, "Balanced load").otherwise("underutilized"))

    val grpNewWorkLoadDF = newWorkLoadDF.groupBy(col("workload")).agg(count("workload").alias("workloadCount"))

    grpNewWorkLoadDF.show()








  }
}