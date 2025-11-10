package exceptions;

public class IncorrectBTreePageType extends RuntimeException {
  public IncorrectBTreePageType(byte type) {
    super(String.format("The b-tree page type %x is not supported", type));
  }
}
