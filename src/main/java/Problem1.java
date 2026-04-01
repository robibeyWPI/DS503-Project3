import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Problem1 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Problem1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> T = spark.read()
                .option("header", "false")
                .option("inferSchema", "true")
                .csv("file:///home/cs585/shared_folder/project1/transactions.csv");

        T = T.toDF(
                "TransID",
                "CustID",
                "TransTotal",
                "TransNumItems",
                "TransDesc"
        );

        Dataset<Row> T1 = T.filter("TransTotal >= 200");

        Dataset<Row> T2 = T1.groupBy("TransNumItems").agg(
                sum("TransTotal").alias("sum_total"),
                avg("TransTotal").alias("avg_total"),
                min("TransTotal").alias("min_total"),
                max("TransTotal").alias("max_total")
        );
        T2.show();

        Dataset<Row> T3 = T1.groupBy("CustID").count().withColumnRenamed("count", "t3_count");
        T3.show();

        Dataset<Row> T4 = T.filter("TransTotal >= 600");

        Dataset<Row> T5 = T4.groupBy("CustID").count().withColumnRenamed("count", "t5_count");

        Dataset<Row> joined = T3.join(T5, "CustID", "left");
        joined = joined.na().fill(0, new String[]{"t5_count"});

        Dataset<Row> T6 = joined.filter("t5_count * 5 < t3_count").select("CustID");
        T6.show();

        spark.stop();
    }
}