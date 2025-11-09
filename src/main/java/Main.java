import executors.DbInfoExecutor;
import executors.TableExecutor;

public class Main {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      System.exit(1);
    }

    String databaseFilePath = args[0];
    String command = args[1];

    switch (command) {
      case ".dbinfo" -> new DbInfoExecutor().execute(databaseFilePath);
      case ".tables" -> new TableExecutor().execute(databaseFilePath);
      default -> System.out.println("Missing or invalid command passed: " + command);
    }
  }
}
