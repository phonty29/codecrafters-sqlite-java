package storage.struct;

public enum Type {
  TABLE("table"),
  INDEX("index");

  private final String name;

  Type(String name) {
    this.name = name;
  }

  public static Type fromName(String name) {
    for (Type type : Type.values()) {
      if (type.name.equals(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown struct type: " + name);
  }
}
