package struct.cells;

import java.nio.ByteBuffer;

public class InteriorTableCell implements Cell {

  private final int rootPage;
  private final int rowId;

  public InteriorTableCell(ByteBuffer cellBuffer) {
    this.rootPage = cellBuffer.getInt();
    this.rowId = readVarInt(cellBuffer);
  }

  @Override
  public int getRowId() {
    return rowId;
  }

  public int getRootPage() {
    return rootPage;
  }
}
