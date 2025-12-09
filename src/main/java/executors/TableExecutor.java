package executors;

import struct.db.DatabaseProducer;

public class TableExecutor implements Executor {

  @Override
  public void execute() {
    var database = DatabaseProducer.get();
    System.out.println(String.join(" ",
        database.getTables().stream().map(table -> table.meta().name()).toList()));
  }
}
