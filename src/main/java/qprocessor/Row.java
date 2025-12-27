package qprocessor;

import java.util.Map;

public class Row {

  private final Map<String, Object> row;

  public Row(Map<String, Object> row) {
    this.row = row;
  }

  public Object get(String key) {
    return this.row.get(key);
  }
}
