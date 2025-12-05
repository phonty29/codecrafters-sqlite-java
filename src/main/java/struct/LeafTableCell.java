package struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LeafTableCell implements Cell {

  private final int recordSize;
  private final int rowId;
  private final RecordHeader recordHeader;
  private final RecordBody recordBody;

  public LeafTableCell(ByteBuffer cellBuffer) {
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

  public int getRowId() {
    return rowId;
  }

  public RecordHeader getRecordHeader() {
    return this.recordHeader;
  }

  public RecordBody getRecordBody() {
    return this.recordBody;
  }

  private int getSizeFromSerialType(int serialType) {
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
