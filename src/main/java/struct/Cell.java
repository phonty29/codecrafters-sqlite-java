package struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Cell {

  private final int recordSize;
  private final int rowId;
  private final RecordHeader recordHeader;
  private final RecordBody recordBody;

  public Cell(ByteBuffer cellBuffer) {
    this.recordSize = readVarInt(cellBuffer);
    this.rowId = readVarInt(cellBuffer);
    // Payload
    // Payload Header
    int payloadHeaderSize = readVarInt(cellBuffer);
    // Get serial types (i.e. size of each column)
    // Until payloadHeaderSize - 1, because payload header is self included
    List<Integer> serialTypes = new ArrayList<>();
    int payloadInitPosition = cellBuffer.position();
    while (cellBuffer.position() - payloadInitPosition < payloadHeaderSize - 1) {
      int serialType = getSizeFromSerialType(readVarInt(cellBuffer));
      serialTypes.add(serialType);
    }
    this.recordHeader = new RecordHeader(payloadHeaderSize, serialTypes);
    // Get values by serial type
    byte[][] values = new byte[serialTypes.size()][];
    for (int i = 0; i < serialTypes.size(); i++) {
      byte[] valueBytes = new byte[serialTypes.get(i)];
      cellBuffer.get(valueBytes);
      values[i] = valueBytes;
    }
    this.recordBody = new RecordBody(values);
  }

  public RecordHeader getRecordHeader() {
    return this.recordHeader;
  }

  public RecordBody getRecordBody() {
    return this.recordBody;
  }

  private int getSizeFromSerialType(int serialType) {
    if (serialType >= 13 && (serialType % 2 == 1)) {
      return (serialType - 13) / 2;
    } else if (serialType >= 12) {
      return (serialType - 12) / 2;
    }
    return serialType;
  }

  // Warning! This method has side effects. Refactor it to immutability of cellBuffer
  private int readVarInt(ByteBuffer cellBuffer) {
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

  public record RecordHeader(
      int payloadHeaderSize,
      List<Integer> serialTypes
  ) {

  }

  public record RecordBody(
      byte[][] values
  ) {

  }
}
