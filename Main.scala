import org.apache.spark.sql.SparkSession
//all the tables
//Branch1Constotcount3 table: drink, totalcount
//Branch1Branch table: drink, branch
//Branch1branchcount:  drink, count
//Branch10branch8branch1drink:  drink, branch

//countDrink is count of drinks count is count of consumers
object Main extends App {
  //def main(args: Array[String]): Unit = {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  //dynamic linking of libraries
  val spark = SparkSession.builder()
    .appName("HiveTest5")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  println("created spark session")
  //all 6 tables created on THursday at 10-16 am
  //  spark.sql("create table if not exists Bev_BranchA3(drink String, branch String) row format delimited fields terminated by ','");
  //  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Bev_BranchA3")
  //  spark.sql("create table if not exists Bev_BranchB3(drink String, branch String) row format delimited fields terminated by ','");
  //  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' OVERWRITE INTO TABLE Bev_BranchB3")
  //  spark.sql("create table if not exists Bev_BranchC3(drink String, branch String) row format delimited fields terminated by ','");
  //  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' OVERWRITE INTO TABLE Bev_BranchC3")
  //THursday  new table names BevcountA3, BevcountB3, BevcountC3
  //  spark.sql("create table if not exists BevcountA3(drink String, count Int) row format delimited fields terminated by ','");
  //  spark.sql("LOAD DATA LOCAL INPATH 'input/BevcountA.txt' OVERWRITE INTO TABLE BevcountA3")
  //  spark.sql("create table if not exists BevcountB3(drink String, count Int) row format delimited fields terminated by ','");
  //  spark.sql("LOAD DATA LOCAL INPATH 'input/BevcountB.txt' OVERWRITE INTO TABLE BevcountB3")
  //  spark.sql("create table if not exists BevcountC3(drink String, count Int) row format delimited fields terminated by ','");
  //  spark.sql("LOAD DATA LOCAL INPATH 'input/BevcountC.txt' OVERWRITE INTO TABLE BevcountC3")
//  spark.sql("SELECT * FROM Bev_BranchA3").show(5)
//  spark.sql("SELECT * FROM Bev_BranchB3").show(5)
//  spark.sql("SELECT * FROM Bev_BranchC3").show(5)
//  spark.sql("SELECT * FROM BevcountA3").show(5)
//  spark.sql("SELECT * FROM BevcountB3").show(5)
//  spark.sql("SELECT * FROM BevcountC3").show(5)


  //Codes to create merged consCount A, B, and C table - Friday
  // spark.sql("DROP TABLE BevcountABC")
  //spark.sql("CREATE TABLE if not  exists BevcountABC3(drink String, countDrink String) row format delimited fields terminated by ','");
  //spark.sql("LOAD DATA LOCAL INPATH 'input/BevcountA.txt' OVERWRITE INTO TABLE BevcountABC3")
  //spark.sql("LOAD DATA LOCAL INPATH 'input/BevcountB.txt' INTO TABLE BevcountABC3")
  //spark.sql("LOAD DATA LOCAL INPATH 'input/BevcountC.txt' INTO TABLE BevcountABC3")
  //Question 1: what is the number of consumer for Branch1?
  //lines 61 to 74 worked
  //Github: create a table called Branch1Branch3 from previous tables where branch name is equal to Branch 1
  // spark.sql("CREATE TABLE if not  exists Branch1Branch3 AS SELECT * FROM Bev_BranchA3 where branch = 'Branch1'");
  // spark.sql("INSERT INTO TABLE Branch1Branch3 SELECT * FROM Bev_BranchB3 where branch = 'Branch1'");
  // spark.sql("INSERT INTO TABLE Branch1Branch3 SELECT * FROM Bev_BranchC3 where branch = 'Branch1'");

  //*NOT DOING it this way  3 table way: CREATE a 2nd table Branch1Branchcount3 which merges tables which has no of consumer with drinks
  // spark.sql("CREATE TABLE if not exists Branch1Branchcount3(drink String, count Int)")
  // spark.sql("INSERT INTO table Branch1Branchcount3 SELECT BevcountA3.drink, sum(BevcountA3.count) from Branch1Branch3 JOIN BevcountA3 ON (Branch1Branch3.drink = BevcountA3.drink) GROUP by BevcountA3.drink")
  // spark.sql("INSERT INTO table Branch1Branchcount3 SELECT BevcountB3.drink, sum(BevcountB3.count) from Branch1Branch3 JOIN BevcountB3 ON (Branch1Branch3.drink = BevcountB3.drink) GROUP by BevcountB3.drink")
  // spark.sql("INSERT INTO table Branch1Branchcount3 SELECT BevcountC3.drink, sum(BevcountC3.count) from Branch1Branch3 JOIN BevcountC3 ON (Branch1Branch3.drink = BevcountC3.drink) GROUP by BevcountC3.drink")
  //spark.sql("SELECT SUM(count) from Branch1Branchcount3").show()
  //sume of count = 6695844
  //count=6695844
  //spark.sql("SELECT * from Branch1Branchcount3").show(75)

  // Question 1: ONE PHYSICAL TABLE -DO NOT  RUN LINE  66 AGAIN-1115974 at 10-50 am
  //spark.sql("CREATE TABLE if not exists Branch1Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch1' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch1' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch1') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch1' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch1' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch1') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch1' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch1' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch1') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
  //spark.sql("SELECT SUM(totalcount) FROM Branch1Constotcount3").show(75)
 // spark.sql("SELECT * FROM Branch1Constotcount3").show(80)
  // totalcount = 1115974
  //preparing for Questions 5. Branch 8 part: DO NOT RUN LINE 72 AGAIN
  // spark.sql("CREATE TABLE if not exists Branch8Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch8' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch8') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch8' UNION ALL SELECT * FROM Bev_BranchC where branch ='Branch8') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch8' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch8') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
  // spark.sql("SELECT SUM(totalcount) FROM Branch8Constotcount3 GROUP BY drink ORDER BY drink").show(50)
 // spark.sql("SELECT * FROM Branch8Constotcount3").show(80)

  //preparing for Questions 5. Branch 4 part
  spark.sql("CREATE TABLE if not exists Branch4Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch4' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch4' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch4') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch4' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch4' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch8') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch4' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch4' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch4') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
  spark.sql("SELECT SUM(totalcount) FROM Branch4Constotcount3 GROUP BY drink ORDER BY drink").show(50)
  spark.sql("SELECT * FROM Branch4Constotcount3").show(80)

  //preparing for Questions 5. Branch 7 part
  spark.sql("CREATE TABLE if not exists Branch7Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch7' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch7') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch7' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch7' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch7') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch7' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch7' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch7') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
  spark.sql("SELECT SUM(totalcount) FROM Branch7Constotcount3 GROUP BY drink ORDER BY drink").show(50)
  spark.sql("SELECT * FROM Branch7Constotcount3").show(80)
  //Question 2 : what is the number of consumer for Branch2?
//  spark.sql("CREATE TABLE if not  exists Branch2Branch3 AS SELECT * FROM Bev_BranchA3 where branch = 'Branch2'");
//  spark.sql("INSERT INTO TABLE Branch2Branch3 SELECT * FROM Bev_BranchB3 where branch = 'Branch2'");
//  spark.sql("INSERT INTO TABLE Branch2Branch3 SELECT * FROM Bev_BranchC3 where branch = 'Branch2'");
//  spark.sql("CREATE TABLE if not exists Branch2Branchcount3(drink String, count Int)")
//  spark.sql("INSERT INTO table Branch2Branchcount3 SELECT BevcountA3.drink, sum(BevcountA3.count) from Branch2Branch3 JOIN BevcountA3 ON (Branch2Branch3.drink = BevcountA3.drink) GROUP by BevcountA3.drink")
//  spark.sql("INSERT INTO table Branch2Branchcount3 SELECT BevcountB3.drink, sum(BevcountB3.count) from Branch2Branch3 JOIN BevcountB3 ON (Branch2Branch3.drink = BevcountB3.drink) GROUP by BevcountB3.drink")
//  spark.sql("INSERT INTO table Branch2Branchcount3 SELECT BevcountC3.drink, sum(BevcountC3.count) from Branch2Branch3 JOIN BevcountC3 ON (Branch2Branch3.drink = BevcountC3.drink) GROUP by BevcountC3.drink")
//  //lines 104 and 105 worked
//  spark.sql("SELECT * from Branch2Branchcount3").show(80)
//  spark.sql("SELECT SUM(count) from Branch2Branchcount3").show()
  //sum of count for Branch2branchcount = 5099141
  //output =  5099141  - do not run these codes again
  //Question 3: What is the most consumed beverage on Branch1?
  //line 111 worked
  //spark.sql("SELECT drink, sum(totalcount) totcount FROM Branch1Constotcount3 GROUP BY drink").show(75)
  //result:  Special_cappuccino=  108163
  //does line 114 work? where i paused on monday
 // spark.sql("SELECT drink, sum(totalcount) totcount FROM Branch8Constotcount GROUP BY drink").show(75)


  //Question 3: What is the least consumed beverage on Branch2?
 // spark.sql("CREATE TABLE if not exists Branch2Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch2' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch2' UNION ALL SELECT * FROM Bev_BranchC where branch ='Branch2') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch2' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch2' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch2') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch2' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch2' UNION ALL SELECT * FROM Bev_BranchC where branch ='Branch2') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
//  spark.sql("SELECT SUM(totalcount) FROM Branch2Constotcount3").show()
//  spark.sql("SELECT drink, sum(totalcount) totcount FROM Branch2Constotcount3  GROUP BY drink ORDER BY totcount DESC LIMIT 1").show(1)
  //Miid_Capuccino 3288074
//  spark.sql("SELECT drink, sum(totalcount) totcount FROM Branch2Constotcount3  GROUP BY drink ORDER BY totcount ASC").show(1)
  //cold mocha 47524
  //Question 5: Average of beverage count in Branch 2
  spark.sql("SELECT * FROM Branch2Constotcount3").show(5)
  spark.sql("SELECT ROUND(avg(totalcount)) FROM Branch2Constotcount3").show()
  //Question 6: Beverage on branch 10, 8 and 1

  spark.sql("CREATE TABLE if not exists Branch10Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch10' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch10' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch10') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA where branch = 'Branch10' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch10' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch10') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch10' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch10' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch10') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
  spark.sql("SELECT SUM(totalcount) FROM Branch10Constotcount3").show()
  spark.sql("SELECT * FROM Branch10Constotcount3").show(75)
  spark.sql("CREATE TABLE if not exists Branch8Constotcount3 AS SELECT drink,totalcount FROM (SELECT BevcountA3.drink,sum(BevcountA3.count) totalcount from (SELECT drink,branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch8' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch8') UNIONRESULT) a JOIN BevcountA3 on (a.drink=BevcountA3.drink) GROUP BY BevcountA3.drink union all SELECT BevcountB3.drink,sum(BevcountB3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch8' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch8') UNIONRESULT) b JOIN BevcountB3 on (b.drink=BevcountB3.drink) GROUP BY BevcountB3.drink UNION ALL SELECT BevcountC3.drink,sum(BevcountC3.count) totalcount from (SELECT drink, branch FROM (SELECT * FROM Bev_BranchA3 where branch = 'Branch8' UNION ALL SELECT * from Bev_BranchB3 where branch = 'Branch10' UNION ALL SELECT * FROM Bev_BranchC3 where branch ='Branch8') UNIONRESULT) c JOIN BevcountC3 on (c.drink=BevcountC3.drink) GROUP BY BevcountC3.drink) UNIONRESULT")
  spark.sql("SELECT SUM(totalcount) FROM Branch8Constotcount3").show()
  spark.sql("SELECT * FROM Branch10Constotcount3").show(75)
 // Branch10, Branch 8 and Branch 1 lines 139 and 130 worked - from Github
//  spark.sql("CREATE TABLE IF NOT EXISTS Branch10Branch8Branch1drink3 AS SELECT drink, branch from (SELECT * from Bev_BranchA3 where branch ='Branch10' or branch ='Branch8' or branch ='Branch1' UNION ALL SELECT * FROM Bev_BranchB3 where branch = 'Branch10' or branch ='Branch8' or branch = 'Branch1' UNION ALL SELECT * FROM Bev_BranchC3 where branch = 'Branch10' or branch = 'Branch8' or branch ='Branch1')UnionResult")
//   spark.sql("SELECT * FROM Branch10Branch8Branch1drink3 order by drink").show(75)
//  spark.sql("SELECT * FROM Branch10Branch8Branch1drink3").show(100)
  println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
  spark.sql("CREATE TABLE IF NOT EXISTS Branch4Branch7drink3 AS SELECT drink, branch from (SELECT * from Bev_BranchB3 where branch ='Branch4' or branch ='Branch7' UNION SELECT * FROM Bev_BranchC3 where branch = 'Branch4' or branch ='Branch7')UNIONTRESULT")
  spark.sql("SELECT * FROM Branch4Branch7drink3 order by drink").show(75)
  spark.sql("SELECT * FROM Branch4Branch7drink3").show(100)
  //spark.sql("DROP VIEW myview1")
  //spark.sql("CREATE VIEW myview2 AS SELECT * FROM Branch10Branch8Branch1drink")
  //spark.sql("SELECT * FROM myview2").show(50)
  //create partition : create partition table, insert your table into the new partition table
//  spark.sql("CREATE TABLE IF NOT EXISTS PartitionT(drink STRING) PARTITIONED BY (branch STRING)")
//  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//  spark.sql("INSERT OVERWRITE TABLE PartitionT PARTITION(branch) SELECT drink,branch from Branch10Branch8Branch1drink")
//  //2 ways of doing it
//  //spark.sql("ALTER TABLE Branch10Branch8Branch1drink SET TBLPROPERTIES ('comment' = 'MONDAY NOTES FROM TEST!!!!!!!!!!!1')").show()
//  spark.sql("ALTER TABLE PartitionT SET TBLPROPERTIES ('notes' = 'MONDAY NOTES FROM TEST!!!!!!!!!!!1')").show()
//  spark.sql("SHOW TBLPROPERTIES PartitionT").show()
//  spark.sql("SELECT * FROM PartitionT").show()
//  spark.sql("DESCRIBE FORMATTED PartitionT").show()
  //try note or notes
  //group scenarios together. label on top
  //REMOVE A ROW from any scenario - per Bryan

   spark.sql("create table if not exists Bev_BranchA3_DeleteTest3(drink String, branch String) row format delimited fields terminated by ','");
   spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Bev_BranchA3_DeleteTest3")
  //RESULT OF FIRST 3 ROWS OF TABLE
  spark.sql("SELECT * FROM Bev_BranchA3_DeleteTest3").show(3)
  //1. create copy table
  spark.sql("CREATE TABLE if not exists Bev_BranchA3_DeleteTest3_temp LIKE Bev_BranchA3_DeleteTest3")
  //2. load data into copy table except deleted item
  spark.sql("INSERT INTO Bev_BranchA3_DeleteTest3_temp SELECT * FROM Bev_BranchA3_DeleteTest3 WHERE drink NOT IN (SELECT drink FROM Bev_BranchA3_DeleteTest3 WHERE drink='MED_LATTE')")
  //overwrite copy table to original table
  spark.sql("INSERT OVERWRITE TABLE Bev_BranchA3_DeleteTest3 SELECT * FROM Bev_BranchA3_DeleteTest3_temp")
  //drop copy table
  spark.sql("DROP TABLE Bev_BranchA3_DeleteTest3_temp")
  //show new table with deleted row
  spark.sql("SELECT * FROM Bev_BranchA3_DeleteTest3").show(3)

}

//Codes to create merged Branch A, B, and C table
// spark.sql("CREATE TABLE if not  exists Bev_BranchABC3(drink String, branch String) row format delimited fields terminated by ','");
// spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Bev_BranchABC3")
// spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Bev_BranchABC3")
// spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Bev_BranchABC3")
//codes below work!!!
// spark.sql("SELECT * from Bev_BranchABC3").show()
// spark.sql("SELECT * from BevcountABC3").show()

//spark.sql("SELECT * from Bev_BranchABC").show()
// spark.sql("SELECT * FROM BevcountABC3 JOIN Bev_BranchABC3 ON   ")

// spark.sql("SELECT drink From Bev_BranchABC3 WHERE branch = 'Branch1'")


//result = 73634
//make a table with new column
// spark.sql("select sum(countDrink) from (SELECT drink From Bev_BranchABC3 WHERE branch = 'Branch1') where drink='SMALL_Espresso' GROUP BY drink")
// spark.sql("SELECT count(*) FROM Bev_BranchABC3 LEFT JOIN BevcountABC3 ON Bev_BranchABC3.drink = BevcountABC3.drink").show()
//result = 73634
//spark.sql("SELECT count(*) from Bev_BranchABC3 WHERE branch='Branch1'").show()
//spark.sql("SELECT count(*) from BevcountABC3").show()
//  val spark = SparkSession.builder
//    .master("local[*]")
//    .appName("Spark Word Count")
//    .getOrCreate()


