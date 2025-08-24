package big.data.cc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import java.io.InputStream;
import java.util.Properties;

public class AppQ3 {
  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName("Q3").setMaster("local[*]");
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
      // prérequis : la vue enedis_sobriete est déjà chargée
      Dataset<Row> df = EnedisDataSet.load(spark,
          "https://www.data.gouv.fr/fr/datasets/r/eedd0b42-6152-4c94-92ba-1d9a7bc8fd91",
          "enedis_sobriete");

      Properties props = new Properties();
      try (InputStream in = AppQ3.class.getClassLoader().getResourceAsStream("database.properties")) {
        props.load(in);
      }
      EnedisDataSet.setProps(props);

      String[] keys = {"sql.residentiels.ref","sql.professionnels.ref","sql.entreprises.ref"};
      for (String k : keys) {
        String view = EnedisDataSet.compute(spark, k);
        System.out.println("\n# Q3 – " + k + " → vue: " + view);
        spark.sql("SELECT * FROM " + view + " ORDER BY date_fin LIMIT 10").show(10, false);
        long c = spark.sql("SELECT COUNT(*) AS c FROM " + view).first().getLong(0);
        System.out.println("Rows: " + c);
      }
    }
  }
}
