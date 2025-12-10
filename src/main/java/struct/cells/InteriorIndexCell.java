package struct.cells;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class InteriorIndexCell implements IndexCell {
  private final int leftChildPointer;
  // rowId of the cell, not of the index
  private final int rowId;
  private final RecordHeader recordHeader;
  private final RecordBody recordBody;

  public InteriorIndexCell(ByteBuffer cellBuffer) {
    this.leftChildPointer = cellBuffer.getInt();
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

  @Override
  public int getRowId() {
    return this.rowId;
  }

  public RecordHeader getRecordHeader() {
    return this.recordHeader;
  }

  public RecordBody getRecordBody() {
    return this.recordBody;
  }
}
