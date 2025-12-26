import executors.ExecutionEngine;
import storage.db.DatabaseProducer;

public class Main {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      System.exit(1);
    }

    String databaseFilePath = args[0];
    String command = args[1];

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");
    DatabaseProducer.init(databaseFilePath);
    new ExecutionEngine(command).execute();
  }
}
