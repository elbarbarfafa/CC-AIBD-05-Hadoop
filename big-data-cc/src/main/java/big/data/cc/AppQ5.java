package big.data.cc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import java.io.InputStream;
import java.util.Properties;

public class AppQ5 {
  public static void main(String[] args) throws Exception {
    final String REGION = System.getProperty("region", "Bretagne");     // -Dregion=...
    final String CATEG  = System.getProperty("categorie", "Entreprises"); // -Dcategorie=...

    SparkConf conf = new SparkConf().setAppName("Q5").setMaster("local[*]");
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {

      System.out.printf("\n=== Q5 | Région=%s | Catégorie=%s ===\n", REGION, CATEG);

      // 1) Enedis (réutilisé)
      EnedisDataSet.load(spark,
          "https://www.data.gouv.fr/fr/datasets/r/eedd0b42-6152-4c94-92ba-1d9a7bc8fd91",
          "enedis_sobriete");
      System.out.println("# Enedis chargée -> vue 'enedis_sobriete'");

      // 2) Charger les propriétés
      Properties p = new Properties();
      try (InputStream in = AppQ5.class.getClassLoader().getResourceAsStream("database.properties")) {
        if (in == null) {
          throw new IllegalStateException(
              "Fichier 'database.properties' introuvable dans src/main/resources");
        }
        p.load(in);
      }

      // Colonnes météo attendues (tu peux les surcharger dans le .properties)
      p.putIfAbsent("meteo.col.datetime",    "Date");
      p.putIfAbsent("meteo.col.region",      "region (name)");
      p.putIfAbsent("meteo.col.temperature", "Température (°C)");
      // Nom de la vue brute
      p.putIfAbsent("meteo.view.raw", "meteo_raw");

      // Vérifs minimales
      String meteoUrl  = p.getProperty("meteo.url");
      String meteoView = p.getProperty("meteo.view.raw");
      if (meteoUrl == null || meteoUrl.isBlank()) {
        throw new IllegalStateException("Propriété manquante: meteo.url");
      }
      if (meteoView == null || meteoView.isBlank()) {
        throw new IllegalStateException("Propriété manquante: meteo.view.raw");
      }

      // 3) Charger météo (IMPORTANT: récupérer le Dataset retourné)
      Dataset<Row> meteoDf = EnedisDataSet.loadMeteoCsv(spark, meteoUrl, meteoView);
      if (meteoDf == null) {
        throw new IllegalStateException("Le loader météo a renvoyé null (vérifie loadMeteoCsv).");
      }
      System.out.println("# Météo chargée -> vue '" + meteoView + "'");
      System.out.println("# Schéma météo:");
      meteoDf.printSchema();
      System.out.println("# Aperçu météo:");
      meteoDf.show(10, false);

      // 4) Pousser les props côté utilitaire si besoin ailleurs
      EnedisDataSet.setProps(p);

      // 5) Agrégation météo filtrée sur la région
      String sqlAgg = p.getProperty("sql.meteo.aggregate");
      if (sqlAgg == null || sqlAgg.isBlank()) {
        // Requête par défaut si rien n'est fourni
        String dtCol = p.getProperty("meteo.col.datetime");
        String regCol = p.getProperty("meteo.col.region");
        String tCol  = p.getProperty("meteo.col.temperature");

        sqlAgg =
          "CREATE OR REPLACE TEMP VIEW meteo_agg AS\n" +
          "SELECT `" + regCol + "` AS region,\n" +
          "       weekofyear(to_timestamp(`" + dtCol + "`)) AS week,\n" +
          "       year(to_timestamp(`" + dtCol + "`))       AS annee,\n" +
          "       AVG(`" + tCol + "`) AS temp_moy\n" +
          "FROM " + meteoView + "\n" +
          "WHERE `" + regCol + "` = '${REGION}'\n" +
          "GROUP BY region, annee, week";
      }
      sqlAgg = sqlAgg.replace("${REGION}", REGION);
      spark.sql(sqlAgg);

      // 6) Jointure avec la table de référence/catégorie
      String sqlRegional = p.getProperty("sql.regional.ref");
      if (sqlRegional == null || sqlRegional.isBlank()) {
        // Par défaut: exemple simple, adapte à ta structure de 'enedis_sobriete'
        // On suppose que Enedis a des colonnes 'region', 'annee', 'week' et une métrique par catégorie
        sqlRegional =
          "SELECT e.region, e.annee, e.week, e.categorie, e.valeur, m.temp_moy\n" +
          "FROM (\n" +
          "  SELECT region, annee, week, categorie, valeur\n" +
          "  FROM enedis_sobriete\n" +
          "  WHERE categorie = '${CATEGORIE}' AND region = '${REGION}'\n" +
          ") e\n" +
          "LEFT JOIN meteo_agg m\n" +
          "  ON e.region = m.region AND e.annee = m.annee AND e.week = m.week";
      }
      sqlRegional = sqlRegional.replace("${REGION}", REGION)
                               .replace("${CATEGORIE}", CATEG);

      Dataset<Row> out = spark.sql(sqlRegional);

      System.out.printf("\n=== Résultat Q5 | Région=%s | Catégorie=%s ===\n", REGION, CATEG);
      out.show(50, false);

      // 7) Export PostgreSQL (optionnel)
      try {
        EnedisDataSet.saveToPostgres(
            out,
            p.getProperty("jdbc.url", ""),
            p.getProperty("jdbc.user", ""),
            p.getProperty("jdbc.password", ""),
            p.getProperty("jdbc.table", "public.result_q5")
        );
        System.out.println("# Q5 – Export PostgreSQL : OK");
      } catch (Exception ex) {
        System.out.println("# Q5 – Export PostgreSQL ignoré : " + ex.getMessage());
      }
    }
  }
}
