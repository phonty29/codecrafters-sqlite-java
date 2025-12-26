package utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class ByteUtils {

  public static Number toNumber(byte[] bytes) {
    if (bytes.length == 0)
      return 0;

    if (bytes.length <= 8) {
      long result = 0;
      for (byte b : bytes) {
        result = (result << 8) | (b & 0xFF);
      }
      return result;
    }

    if (bytes.length <= 12) {
      return new BigInteger(1, bytes); // 1 = unsigned
    }

    throw new IllegalArgumentException("Too many bytes: " + bytes.length);
  }
}
