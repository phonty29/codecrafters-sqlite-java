package executors;

public class ExecutionEngine implements Executor {
  private final String command;

  public ExecutionEngine(String command) {
    this.command = command;
  }

  @Override
  public void execute() {
    switch (command) {
      case ".dbinfo" -> new DbInfoExecutor().execute();
      case ".tables" -> new TableExecutor().execute();
      case String s when s.toLowerCase().startsWith("select") ->
          new QueryExecutor(command).execute();
      default -> System.err.printf("Missing or invalid command passed: %s%n", command);
    }
  }
}
