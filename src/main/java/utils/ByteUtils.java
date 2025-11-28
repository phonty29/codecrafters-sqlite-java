package utils;

import java.nio.ByteBuffer;

public class ByteUtils {

  public static Number toInteger(byte[] bytes) {
    return switch (bytes.length) {
      case 0 -> 0;
      case 1 -> ByteBuffer.wrap(bytes).get();
      case 2 -> ByteBuffer.wrap(bytes).getShort();
      case 4 -> ByteBuffer.wrap(bytes).getInt();
      case 8 -> ByteBuffer.wrap(bytes).getLong();
      default -> throw new IllegalStateException("Unexpected value: " + bytes.length);
    };
  }

  public static Number toReal(byte[] bytes) {
    return switch (bytes.length) {
      case 0 -> 0;
      case 4 -> ByteBuffer.wrap(bytes).getFloat();
      case 8 -> ByteBuffer.wrap(bytes).getDouble();
      default -> throw new IllegalStateException("Unexpected value: " + bytes.length);
    };
  }
}
