package struct;

import java.nio.ByteBuffer;

public interface Cell {
  // REFACTOR! This method has side effects. Turn it to immutability of cellBuffer
  default int readVarInt(ByteBuffer cellBuffer) {
    int result = 0;
    for (int i = 0; i < 8; i++) {
      final byte current = cellBuffer.get();
      result = (result << 7) + (current & 0x7F);
      if ((current & 0x80) == 0) {
        return result;
      }
    }
    final byte last = cellBuffer.get();
    result = (result << 8) + last;
    return result;
  }
}
