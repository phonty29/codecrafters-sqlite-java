package executors;

import java.util.Arrays;
import struct.db.DatabaseProducer;

public class TableExecutor implements Executor {

  @Override
  public void execute() {
    var database = DatabaseProducer.get();
    System.out.println(String.join(" ",
        Arrays.stream(database.getTables()).map(table -> table.meta().name()).toList()));
  }
}
