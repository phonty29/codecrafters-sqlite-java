package storage.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class DatabaseProducer {

  private static Database database;

  public static void init(String databaseFilePath) {
    if (Objects.nonNull(database)) {
      throw new IllegalStateException("Database already initialized");
    }
    try {
      database = new Database(Path.of(databaseFilePath));
    } catch (IOException x) {
      System.err.println(x.getMessage());
    }
  }

  public static Database get() {
    if (Objects.nonNull(database)) {
      return database;
    }
    throw new IllegalStateException("Database is not initialized");
  }

}
