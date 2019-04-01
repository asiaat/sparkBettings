README
-----------------------------------------------------------------
This is the solution for Code Challenge project.
The input data is at the input folder
The precalculated results(csv files) are at the docs folder

1. Installation
----------------------------
Unzip the project and open it with IntelliJi IDEA


2. Configuration and compilation
------------------------------------
On the IntelliJ terminal, run the mvn command with those options
$ mvn clean package


PROBLEM 1 Balance since registration
--------------------------------------------------------------

1. To start the calculations run :
/spark-submit --class com.koproj.scala.CustomerBalance --master local target/sparkBettings-1.0-SNAPSHOT.jar

2. the result file customers_balance.csv will be loaded into output folder


PROBLEM 2 net profit per customer’s country
--------------------------------------------------------------

1. To start the calculations run :
spark-submit --class com.koproj.scala.CountryProfit --master local target/sparkBettings-1.0-SNAPSHOT.jar

2. the result file net_country_profit.csv will be loaded into output folder
