package struct.db;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;

public class DatabaseProducer {

  private static Database database;

  public static void init(FileInputStream databaseFile) throws IOException {
    if (Objects.nonNull(database)) {
      throw new IllegalStateException("Database already initialized");
    }
    database = new Database(databaseFile);
  }

  public static Database get() {
    if (Objects.nonNull(database)) {
      return database;
    }
    throw new IllegalStateException("Database is not initialized");
  }

}
