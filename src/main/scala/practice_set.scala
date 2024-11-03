
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, initcap, to_date, when}

object practice_set{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")

    val resultDF = employees
      .withColumn("last_checkin", to_date(col("last_checkin"), "yyyy-MM-dd"))
      .withColumn("name", initcap(col("name")))
      .withColumn("status", when(datediff(current_date(), col("last_checkin")) <= 7, "Active" ).otherwise("Inactive"))

    resultDF.show()

    employees.createOrReplaceTempView("employees_sql")
    val sql_result = spark.sql(
      """
        |select initcap(name) as name,
        |case when datediff(current_date() - to_date(last_checkin, 'yyyy-MM-dd')) <= 7 then 'Active'
        |else 'Inactive'
        |end as status
        |from employee_sql
        |""".stripMargin)
    sql_result.show()




  }
}