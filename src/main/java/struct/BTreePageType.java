package struct;

public enum BTreePageType {
  INT_INDEX((byte) 2),
  INT_TABLE((byte) 5),
  LEAF_INDEX((byte) 10),
  LEAF_TABLE((byte) 13);

  private byte bTreePageType;

  BTreePageType(byte type) {
    this.bTreePageType = type;
  }

  public static BTreePageType valueOf(byte bTreePageType) {
    for (BTreePageType type : BTreePageType.values()) {
      if (type.bTreePageType == bTreePageType) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid bTreePageType: " + bTreePageType);
  }
}
