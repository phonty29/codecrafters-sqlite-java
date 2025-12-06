package struct.cells;

import java.nio.ByteBuffer;

public interface Cell {

  int getRowId();

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

  default int getSizeFromSerialType(int serialType) {
    if (serialType >= 0 && serialType <= 4) {
      return serialType;
    } else if (serialType == 5) {
      return 6;
    } else if (serialType == 6 || serialType == 7) {
      return 8;
    } else if (serialType >= 13 && serialType % 2 == 1) {
      return (serialType - 13) / 2;
    } else if (serialType >= 12) {
      return (serialType - 12) / 2;
    } else {
      return 0;
    }
  }
}
