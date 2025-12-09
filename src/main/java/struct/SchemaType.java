package struct;

public enum SchemaType {
  TABLE("table"),
  INDEX("index");

  private final String name;

  SchemaType(String name) {
    this.name = name;
  }

  public static SchemaType fromName(String name) {
    for (SchemaType type : SchemaType.values()) {
      if (type.name.equals(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown schema type: " + name);
  }
}
