package storage.cells;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import utils.ByteUtils;

public class LeafIndexCell implements IndexCell {

  private final int payloadSize;
  private final RecordHeader recordHeader;
  private final RecordBody recordBody;

  public LeafIndexCell(ByteBuffer cellBuffer) {
    this.payloadSize = readVarInt(cellBuffer);
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
    return ByteUtils.toNumber(this.recordBody.values()[this.recordBody.values().length - 1])
        .intValue();
  }

  @Override
  public RecordHeader getRecordHeader() {
    return this.recordHeader;
  }

  @Override
  public RecordBody getRecordBody() {
    return this.recordBody;
  }
}