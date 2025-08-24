package big.data.cc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import java.io.InputStream;
import java.util.Properties;

public class AppQ4 {
  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf()
        .setAppName("Q4")
        .setMaster("local[*]");
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {

      Dataset<Row> df = EnedisDataSet.load(spark,
          "https://www.data.gouv.fr/fr/datasets/r/eedd0b42-6152-4c94-92ba-1d9a7bc8fd91",
          "enedis_sobriete");

      Properties props = new Properties();
      try (InputStream in = AppQ4.class.getClassLoader().getResourceAsStream("database.properties")) {
        props.load(in);
      }
      EnedisDataSet.setProps(props);

      String[] cats = {"Résidentiels","Professionnels","Entreprises"};
      for (String cat : cats) {
        // SQL
        String key = cat.equals("Résidentiels") ? "sql.residentiels.ref"
                    : cat.equals("Professionnels") ? "sql.professionnels.ref"
                    : "sql.entreprises.ref";
        long t0 = System.nanoTime();
        String viewSql = EnedisDataSet.compute(spark, key);
        long nSql = spark.sql("SELECT COUNT(*) c FROM " + viewSql).first().getLong(0);
        long sqlMs = (System.nanoTime()-t0)/1_000_000L;

        // API Java
        long t1 = System.nanoTime();
        long nApi = EnedisDataSet.optimizedCompute(spark, df, cat).count();
        long apiMs = (System.nanoTime()-t1)/1_000_000L;

        System.out.printf("\n# Q4 – %s: SQL=%d rows (%d ms) | API=%d rows (%d ms)\n",
            cat, nSql, sqlMs, nApi, apiMs);
      }
    }
  }
}
