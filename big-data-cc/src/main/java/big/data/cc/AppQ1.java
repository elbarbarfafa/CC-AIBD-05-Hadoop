package big.data.cc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class AppQ1 {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Q1 : Vérifications d'environnement ===");
        System.out.println("JAVA_HOME          = " + System.getenv("JAVA_HOME"));
        System.out.println("HADOOP_HOME        = " + System.getenv("HADOOP_HOME"));
        System.out.println("HADOOP_CONF_DIR    = " + System.getenv("HADOOP_CONF_DIR"));
        System.out.println("SPARK_HOME         = " + System.getenv("SPARK_HOME"));

        SparkConf conf = new SparkConf()
                .setAppName("Q1-EnvChecks")
                .setMaster("local[*]")
                .set("spark.eventLog.enabled", "false");
        try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
            System.out.println("Spark version      = " + spark.version());
            System.out.println("Spark UI           = http://localhost:4040 (si UI activée)");
        }
        System.out.println("Q1 terminé.");
    }
}
