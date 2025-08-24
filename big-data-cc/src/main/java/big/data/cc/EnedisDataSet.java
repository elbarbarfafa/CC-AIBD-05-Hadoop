package big.data.cc;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;                 // <-- ajouté
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class EnedisDataSet {

    private static final Logger log = LoggerFactory.getLogger(EnedisDataSet.class);

    // === Q3: stockage des requêtes externalisées ===
    private static Properties props;

    /** Appelée depuis AppMain après avoir chargé database.properties */
    public static void setProps(Properties p) {
        props = p;
    }

    /** Chargement direct depuis le classpath si tu préfères (fallback). */
    public static void loadPropsFromClasspathIfNeeded() {
        if (props != null) return;
        try (InputStream in = EnedisDataSet.class.getClassLoader()
                .getResourceAsStream("database.properties")) {
            if (in == null) {
                log.warn("database.properties introuvable sur le classpath.");
                props = new Properties();
            } else {
                Properties p = new Properties();
                p.load(in);
                props = p;
                log.info("database.properties chargé ({} clés).", props.size());
            }
        } catch (IOException e) {
            log.error("Erreur de chargement de database.properties", e);
            props = new Properties();
        }
    }

    /**
     * Q3 - Exécute la requête identifiée par 'reqKey' depuis database.properties,
     * publie le résultat comme vue temporaire 'result_<reqKey>' dans la session, loggue et renvoie le nom de la vue.
     */
    public static String compute(SparkSession spark, String reqKey) {
        loadPropsFromClasspathIfNeeded();
        Objects.requireNonNull(props, "Props non initialisées");

        String sql = props.getProperty(reqKey);
        if (sql == null) {
            throw new IllegalArgumentException("Clé de requête inconnue: " + reqKey);
        }
        log.info("Exécution requête [{}] : {}", reqKey, sql);

        Dataset<Row> res = spark.sql(sql).cache();
        String viewName = "result_" + reqKey.replace('.', '_');
        res.createOrReplaceTempView(viewName);

        // Logs demandés : aperçu + nombre de lignes
        String preview = res.showString(20, 0, false);
        log.info("Vue '{}' créée. Aperçu:\n{}", viewName, preview);
        log.info("Vue '{}' - nombre de lignes: {}", viewName, res.count());

        return viewName;
    }

    public static Dataset<Row> load(SparkSession spark, String csvUrl, String tableName) {
        try {
            // 1) Télécharger vers un fichier temporaire local
            Path tmp = Files.createTempFile("enedis-", ".csv");
            try (InputStream in = URI.create(csvUrl).toURL().openStream()) {
                Files.copy(in, tmp, REPLACE_EXISTING);
            }
            tmp.toFile().deleteOnExit();
            String localPath = tmp.toAbsolutePath().toString();
            log.info("CSV téléchargé dans le fichier temporaire: {}", localPath);

            // 2) Détection simple du séparateur (',' vs ';')
            String header = Files.readAllLines(tmp, StandardCharsets.UTF_8).get(0);
            char sep = guessSeparator(header);
            log.info("Séparateur détecté: {}", sep == ';' ? "point-virgule (;)" : "virgule (,)");

            // 3) Lecture Spark + cache + publication de la vue temporaire
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("sep", String.valueOf(sep))
                    .csv(localPath)
                    .cache();

            df.createOrReplaceTempView(tableName);
            log.info("Vue temporaire créée dans la session: {}", tableName);
            return df;

        } catch (Exception e) {
            log.error("Échec de chargement du CSV depuis l'URL {}", csvUrl, e);
            throw new RuntimeException(e);
        }
    }

    private static char guessSeparator(String headerLine) {
        long commas = headerLine.chars().filter(c -> c == ',').count();
        long semicolons = headerLine.chars().filter(c -> c == ';').count();
        return semicolons > commas ? ';' : ',';
    }

    public static void logSchema(Dataset<Row> df) {
        String schema = df.schema().treeString();
        log.info("Schéma inféré:\n{}", schema);
    }

    public static void logPreview(Dataset<Row> df, int n) {
        String preview = df.showString(n, 0, false);
        log.info("Aperçu des {} premières lignes:\n{}", n, preview);
    }

    public static void logSessionCatalog(SparkSession spark) {
        String tables = spark.catalog().listTables().toDF().showString(200, 0, false);
        log.info("Tables/vues dans la session:\n{}", tables);
    }

    public static void logRowCount(Dataset<Row> df) {
        long count = df.count();
        log.info("Nombre de lignes: {}", count);
    }

    // ===== helper pour générer des noms de vues sûrs (sans accents / caractères spéciaux)
    private static String sanitizeName(String raw) {
        String nfd = Normalizer.normalize(raw, Normalizer.Form.NFD)
                               .replaceAll("\\p{M}+", "");      // supprime les diacritiques
        String s = nfd.toLowerCase()
                      .replace(' ', '_')
                      .replaceAll("[^a-z0-9_]", "_");          // tout le reste -> "_"
        if (s.isEmpty() || !Character.isLetter(s.charAt(0))) s = "t_" + s; // évite début non lettre
        return s;
    }

    /**
     * Q4 — Version API Java (pas SQL).
     * Pour une catégorie (ex: "Résidentiels"), renvoie les semaines (période ref)
     * où la conso réalisée < conso normale, triées par date_fin croissante.
     * Le résultat est aussi publié comme vue temporaire "result_api_<cat>".
     */
    public static Dataset<Row> optimizedCompute(SparkSession spark, Dataset<Row> base, String categorie) {
        // Colonnes du dataset (d’après ton schéma réel)
        Column seg  = col("`Segment client`");
        Column date = col("date_fin");
        Column week = col("week");
        Column tr   = col("conso_lissee_tr_ref");
        Column tn   = col("conso_lissee_tn_ref");

        Dataset<Row> res = base
                .filter(seg.equalTo(categorie).and(tr.lt(tn)))
                .select(
                        date,
                        week,
                        seg.alias("segment"),
                        tr.alias("conso_tr"),
                        tn.alias("conso_tn")
                )
                .orderBy(date.asc())
                .cache(); // on matérialise pour mesures et réutilisation

        // *** correction : vue sans accents/espaces/caractères interdits
        String view = "result_api_" + sanitizeName(categorie);
        res.createOrReplaceTempView(view);

        // petit log d’aperçu
        String preview = res.showString(20, 0, false);
        log.info("Vue '{}' créée (API Java). Aperçu:\n{}", view, preview);
        log.info("Vue '{}' - nombre de lignes (API Java): {}", view, res.count());

        return res;
    }

	public static void saveToPostgres(Dataset<Row> out, String jdbcUrl, String jdbcTab, String jdbcUser,
			String jdbcPass) {
		// TODO Auto-generated method stub
		
	}

	public static Dataset<Row> loadMeteoCsv(SparkSession spark, String meteoUrl, String meteoView) {
		// TODO Auto-generated method stub
		return null;
	}
}
