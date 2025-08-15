package fr.dawan.paris;

import org.apache.arrow.vector.table.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ValidationPlateformeSpark")
                .master("local[*]")
                .getOrCreate();

        Dataset<org.apache.spark.sql.Row> df = spark.range(10).toDF("id");
        df.show();

        spark.stop();
    }
}