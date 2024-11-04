
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{max,avg, col, count, current_date, datediff, initcap, sum, to_date, when}

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


    ///////////question-2/////////////


    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)

    ).toDF("name", "total_sales")

    val salesDF = sales
      .withColumn("name", initcap(col("name")))
      .withColumn("performance_status", when(col("total_sales") > 50000, "Excellent")
        .when(col("total_sales")>25000 && col("total_sales")<50000, "Good").otherwise("Needs Improvement"))
    val aggSalesdf = salesDF.groupBy(col("performance_status")).agg(sum(col("total_sales"))).alias("agg_sales")
    aggSalesdf.show()

    /////////////question 3///////////////////////////

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

    /////////////question 5/////////////////////////

    val employees1 = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")

    val newEmployees1DF = employees1.withColumn("name", initcap(col("name")))
      .withColumn("overtime_status", when(col("hours_worked") > 60, "Excessive Overtime").when(col("hours_worked")>= 45 && col("hours_worked")<=60, "Standard Overtime").otherwise("No overtime"))
    newEmployees1DF.show()
    val grpEmployee1DF = newEmployees1DF.groupBy(col("overtime_status")).agg(count("name"))
    grpEmployee1DF.show()

    employees1.createOrReplaceTempView("employee1sql")

    val employees1sql = spark.sql(
      """
        |select initcap(name) as name,hours_worked,
        |case when hours_worked > 60 then 'Excessive Overtime'
        |when hours_worked >= 45 and hours_worked <= 60 then 'Standard Overtime'
        |else 'No overtime'
        |end as overtime_status from employees1sql
        |""".stripMargin)

    /////////////////////question 6/////////////////////////

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")

    val customerDF = customers.withColumn("name", initcap(col("name")))
      .withColumn("agegroup", when(col("age")< 25, "Youth").when(col("age")>=45 && col("age")<=25, "Adult").otherwise("Senior"))
    customerDF.show()

    val grpcustomerDF = customerDF.groupBy(col("agegroup")).agg(count("name"))
    grpcustomerDF.show()


    ///////////question7////////////////////
      val vehicles = List(
        ("CarA", 30),
        ("CarB", 22),
        ("CarC", 18),
        ("CarD", 15),
        ("CarE", 10),
        ("CarF", 28),
        ("CarG", 12),
        ("CarH", 35),
        ("CarI", 25),
        ("CarJ", 16)
      ).toDF("vehicle_name", "mileage")
   val vehiclesDF = vehicles.withColumn("efficiency", when(col("mileage")>25, "High Efficiency")
       .when(col("mileage")>=15 && col("mileage")<= 25, "Moderate")
       .otherwise("Low efficiency"))
    vehiclesDF.show()

    //////////////////////////////////question8///////////////////////
    val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")

    val studentsDF = students.withColumn("results", when(col("score")>90, "Excellent")
        .when(col("score")>=75 && col("score")<= 89, "good")
        .otherwise("Needs Improvement"))
    studentsDF.show()

    val grpstudentDF = studentsDF.groupBy(col("results")).agg(count(col("name")))
    grpstudentDF.show()

    ///////////////////question9////////////////////
    val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")

    val inventoryDF = inventory.withColumn("inventory", when(col("stock_quantity")>100, "over stocked")

        .when(col("stock_quantity")>=50 && col("stock_quantity")<= 100,"normal")
        .otherwise("Low Stock"))
    inventoryDF.show()

    val countinventoryDF = inventoryDF.groupBy(col("inventory")).agg(count("product_name"))
    countinventoryDF.show()

    ///////////////////question10//////////////////////////


    val employees2 = List(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)
      ).toDF("name", "department", "performance_score")

    val bonusDF = employees2.withColumn("bonus",
      when((col("department") === "Sales" || col("department") === "Marketing") && col("performance_score") > 80, "Bonus 20%")
        .when(col("performance_score") > 70, "Bonus 15%")
        .otherwise("No bonus")
    )
    val totalBonusDF = bonusDF.groupBy("department").agg(sum("bonus").alias("total_bonus"))

    totalBonusDF.show()

    /////////////////////question11///////////////////////////

    val products = List(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
    ("Shoes", "Clothing", 90, 55),
    ("Jacket", "Clothing", 30, 80),
    ("TV", "Electronics", 150, 40),
    ("Watch", "Accessories", 60, 65),
    ("Pants", "Clothing", 25, 75),
    ("Camera", "Electronics", 95, 58)
    ).toDF("product_name", "category", "return_count", "satisfaction_score")

    val classifiedDF = products.withColumn(
      "return_rate",
      when(col("return_count")>100 && col("satisfaction_score")<50, "High Return Rate")
        .when(col("return_count").between(50, 100) && col("satisfaction_score").between(50, 70), "Moderate Return Rate")
        .otherwise("Low Return Rate")
    )

    val resultDF = classifiedDF
      .groupBy("category", "return_rate").agg(count("product_name").alias("product_count"))

    resultDF.show()
  ///////////////////////////question12////////////////////////////

    val customers = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")



    val CustomersDF = customers.withColumn("spending_level",
      when(col("spending") > 1000 && col("membership") === "Premium", "High Spender")
        .when(col("spending").between(500, 1000) && col("membership") === "Standard", "Average Spender")
        .otherwise("Low Spender")
    )

    val resultDF = CustomersDF
      .groupBy("membership")
      .agg(avg("spending").alias("average_spending"))

    resultDF.show()

    ////////////////////////////question13//////////////////////

    val orders = List(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Smartphone", "Domestic", 3),
      ("Order4", "Tablet", "International", 5),
      ("Order5", "Watch", "Domestic", 7),
      ("Order6", "Headphones", "International", 10),
      ("Order7", "Camera", "Domestic", 1),
      ("Order8", "Shoes", "International", 9),
      ("Order9", "Laptop", "Domestic", 6),
      ("Order10", "Tablet", "International", 4)
    ).toDF("order_id", "product_type", "origin", "delivery_days")

    val OrdersDF = orders.withColumn(
      "delivery_speed",
      when(col("delivery_days") > 7 && col("origin") === "International", "Delayed")
        .when(col("delivery_days").between(3, 7), "On-Time")
        .otherwise("Fast")
    )
    val resultDF = OrdersDF
      .groupBy("product_type", "delivery_speed")
      .agg(count("order_id").alias("order_count"))
      .orderBy("product_type")

    resultDF.show()

    /////////////////////////question 14/////////////////////

    val loanApplicants = List(
      ("karthik", 60000, 120000, 590),
      ("neha", 90000, 180000, 610),
      ("priya", 50000, 75000, 680),
      ("mohan", 120000, 240000, 560),
      ("ajay", 45000, 60000, 620),
      ("vijay", 100000, 100000, 700),
      ("veer", 30000, 90000, 580),
      ("aatish", 85000, 85000, 710),
      ("animesh", 50000, 100000, 650),
      ("nishad", 75000, 200000, 540)
    ).toDF("name", "income", "loan_amount", "credit_score")

    val ApplicantsDF = loanApplicants.withColumn("risk_level",when(col("loan_amount") > 2 * col("income") && col("credit_score") < 600, "High Risk")
        .when(col("loan_amount").between(col("income"), 2 * col("income")) && col("credit_score").between(600, 700), "Moderate Risk")
        .otherwise("Low Risk")
    )
    ApplicantsDF.show()

    /////////////////////////////question 15 //////////////////////////

    val customerPurchases = List(
      ("karthik", "Premium", 50, 5000),
      ("neha", "Standard", 10, 2000),
      ("priya", "Premium", 65, 8000),
      ("mohan", "Basic", 90, 1200),
      ("ajay", "Standard", 25, 3500),
      ("vijay", "Premium", 15, 7000),
      ("veer", "Basic", 75, 1500),
      ("aatish", "Standard", 45, 3000),

      ("animesh", "Premium", 20, 9000),
      ("nishad", "Basic", 80, 1100)
    ).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")

    val CustomersDF = customerPurchases.withColumn("purchase_recency", when(col("days_since_last_purchase") <= 30, "Frequent")
        .when(col("days_since_last_purchase") <= 60, "Occasional")
        .otherwise("Rare"))
    val recencyCountDF = CustomersDF
      .groupBy("membership", "purchase_recency")
      .agg(count("name").alias("customer_count"))
    recencyCountDF.show()

    val avgTotalPurchase = CustomersDF.filter(col("purchase_recency") === "Frequent" && col("membership") === "Premium")
      .agg(avg("total_purchase_amount").alias("avg_total_purchase_amount"))
    avgTotalPurchase.show()

    val minPurchaseAmountRareDF = CustomersDF.filter(col("purchase_recency") === "Rare")
      .groupBy("membership")
      .agg(functions.min("total_purchase_amount").alias("min_total_purchase_amount"))
    minPurchaseAmountRareDF.show()


////////////////////////////question 16/////////////////////////////
val electricityUsage = List(
  ("House1", 550, 250),
    ("House2", 400, 180),
    ("House3", 150, 50),
    ("House4", 500, 200),
    ("House5", 600, 220),
    ("House6", 350, 120),
    ("House7", 100, 30),
    ("House8", 480, 190),
    ("House9", 220, 105),
    ("House10", 150, 60)
    ).toDF("household", "kwh_usage", "total_bill")
    val UsageDF = electricityUsage.withColumn("usage_category", when(col("kwh_usage") > 500 && col("total_bill") > 200, "High Usage")
        .when(col("kwh_usage").between(200, 500) && col("total_bill").between(100, 200), "Medium Usage")
        .otherwise("Low Usage")
    )

    val CountDF = UsageDF.groupBy("usage_category").agg(count("household").alias("household_count"))
    val maxDF = UsageDF.filter(col("usage_category") === "High Usage").agg(max("total_bill").alias("max_bill_amount"))
    val avgmediumUsageDF = UsageDF.filter(col("usage_category") === "Medium Usage").agg(avg("kwh_usage").alias("avg_kwh_usage"))
    val lowDF = UsageDF.filter(col("usage_category") === "Low Usage" && col("kwh_usage") > 300).agg(count("household").alias("low_usage_high_kwh_count"))

    //////////////////////////////question 17////////////////////////////////////

    val employees3 = List(
      ("karthik", "IT", 110000, 12, 88),
      ("neha", "Finance", 75000, 8, 70),
      ("priya", "IT", 50000, 5, 65),
      ("mohan", "HR", 120000, 15, 92),
      ("ajay", "IT", 45000, 3, 50),
      ("vijay", "Finance", 80000, 7, 78),
      ("veer", "Marketing", 95000, 6, 85),
      ("aatish", "HR", 100000, 9, 82),
      ("animesh", "Finance", 105000, 11, 88),
      ("nishad", "IT", 30000, 2, 55)
    ).toDF("name", "department", "salary", "experience", "performance_score")

    val employees3DF = employees3.withColumn("salary_band", when(col("salary") > 100000 && col("experience") > 10, "Senior")
        .when(col("salary").between(50000, 100000) && col("experience").between(5, 10), "Mid-level")
        .otherwise("Junior")
    )
    val BandCountDF = employees3DF.groupBy("department", "salary_band").agg(count("name").alias("count"))
    val avgBandDF = employees3DF.groupBy("salary_band").agg(avg("performance_score").alias("avg_performance_score"))
      .filter(col("avg_performance_score") > 80)
    val midlevelDF = employees3DF
      .filter(col("salary_band") === "Mid-level" && col("performance_score") > 85 && col("experience") > 7)










































  }
}