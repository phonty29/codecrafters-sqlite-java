package struct;

import java.nio.ByteBuffer;

public interface SchemaElement {

  default int getRootPage(byte[] rootPageBytes) {
    return switch (rootPageBytes.length) {
      case 1 -> Byte.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).get());
      case 2 -> Short.toUnsignedInt(ByteBuffer.wrap(rootPageBytes).getShort());
      case 3 -> ((rootPageBytes[0] & 0xFF) << 16) |
          ((rootPageBytes[1] & 0xFF) << 8) |
          (rootPageBytes[2] & 0xFF);
      case 4 -> ByteBuffer.wrap(rootPageBytes).getInt();
      default -> throw new IllegalStateException("Rootpage couldn't be cast to integer type");
    };
  }
}
