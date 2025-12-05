package struct;

import java.nio.ByteBuffer;

public class InteriorTableCell implements Cell {
  private final int rootPage;
  private final int rowId;

  public InteriorTableCell(ByteBuffer cellBuffer) {
    this.rootPage = cellBuffer.getInt();
    this.rowId = readVarInt(cellBuffer);
  }

  public int getRootPage() {
    return rootPage;
  }
}
