package big.data.cc;

public class AppMain {
    public static void main(String[] args) {
        System.out.println(
            "\n=== Lancement par question ===\n" +
            "Utilise Maven en précisant la classe à exécuter, par ex. :\n" +
            "  mvn -q -Dexec.mainClass=big.data.cc.AppQ1 exec:java\n" +
            "  mvn -q -Dexec.mainClass=big.data.cc.AppQ2 exec:java\n" +
            "  mvn -q -Dexec.mainClass=big.data.cc.AppQ3 exec:java\n" +
            "  mvn -q -Dexec.mainClass=big.data.cc.AppQ4 exec:java\n" +
            "  mvn -q -Dexec.mainClass=big.data.cc.AppQ5 exec:java\n"
        );
    }
}
