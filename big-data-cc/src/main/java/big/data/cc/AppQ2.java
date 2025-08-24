package big.data.cc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class AppQ2 {
  public static void main(String[] args) throws Exception {
    final String url   = "https://www.data.gouv.fr/fr/datasets/r/eedd0b42-6152-4c94-92ba-1d9a7bc8fd91";
    final String table = "enedis_sobriete";

    SparkConf conf = new SparkConf().setAppName("Q2").setMaster("local[*]");
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
      Dataset<Row> df = EnedisDataSet.load(spark, url, table);
      System.out.println("\n# Q2 – Vue publiée '" + table + "'");
      EnedisDataSet.logSchema(df);
      EnedisDataSet.logSessionCatalog(spark);
      EnedisDataSet.logPreview(df, 10);
    }
  }
}
