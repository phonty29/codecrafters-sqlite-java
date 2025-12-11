package struct.cells;

import java.nio.ByteBuffer;

public class InteriorTableCell implements Cell {

  private final int leftChildPointer;
  private final int rowId;

  public InteriorTableCell(ByteBuffer cellBuffer) {
    this.leftChildPointer = cellBuffer.getInt();
    this.rowId = readVarInt(cellBuffer);
  }

  @Override
  public int getRowId() {
    return rowId;
  }

  public int getLeftChildPointer() {
    return leftChildPointer;
  }
}
