package big.data.cc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Classe principale :
 * - crée SparkConf + SparkSession
 * - charge le CSV Enedis et publie la vue "enedis_sobriete"
 * - charge les requêtes SQL depuis database.properties
 * - exécute les 3 requêtes Q3 et loggue les résultats
 * - stoppe proprement la session
 */
public class AppMain {

    private static final Logger log = LoggerFactory.getLogger(AppMain.class);

    public static void main(String[] args) throws Exception {
        final String url = "https://www.data.gouv.fr/fr/datasets/r/eedd0b42-6152-4c94-92ba-1d9a7bc8fd91";
        final String table = "enedis_sobriete";

        // 1) SparkConf + SparkSession
        SparkConf conf = new SparkConf().setAppName("EnedisSobrieteApp").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        try {
            log.info("Session Spark démarrée : {}", spark);

            // 2) Charger le dataset et publier la vue
            Dataset<Row> df = EnedisDataSet.load(spark, url, table);

            // Logs Q2
            EnedisDataSet.logSchema(df);
            EnedisDataSet.logSessionCatalog(spark);
            EnedisDataSet.logRowCount(df);
            EnedisDataSet.logPreview(df, 20);

            // 3) Charger les requêtes SQL depuis database.properties
            Properties props = new Properties();
            try (InputStream in = AppMain.class.getClassLoader().getResourceAsStream("database.properties")) {
                if (in == null) {
                    log.warn("database.properties introuvable sur le classpath -> fallback auto");
                    EnedisDataSet.loadPropsFromClasspathIfNeeded();
                } else {
                    props.load(in);
                    log.info("database.properties chargé ({} clés).", props.size());
                    EnedisDataSet.setProps(props);
                }
            }

            // 4) Exécuter les 3 requêtes demandées
            String vRes = EnedisDataSet.compute(spark, "sql.residentiels.ref");
            String vPro = EnedisDataSet.compute(spark, "sql.professionnels.ref");
            String vEnt = EnedisDataSet.compute(spark, "sql.entreprises.ref");

            // 5) (Optionnel) montrer 5 lignes de chaque vue via SQL
            spark.sql("SELECT * FROM " + vRes + " LIMIT 5").show(false);
            spark.sql("SELECT * FROM " + vPro + " LIMIT 5").show(false);
            spark.sql("SELECT * FROM " + vEnt + " LIMIT 5").show(false);

        } catch (Exception e) {
            log.error("Erreur dans AppMain", e);
            throw e;
        } finally {
            log.info("Arrêt de la session Spark...");
            spark.stop();
            log.info("Session Spark arrêtée.");
        }
    }
}
