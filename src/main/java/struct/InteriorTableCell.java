package struct;

import java.nio.ByteBuffer;

public class InteriorTableCell implements Cell {
  private final int rootPage;
  private final int rowId;

  public InteriorTableCell(ByteBuffer cellBuffer) {
    this.rootPage = readVarInt(cellBuffer);
    this.rowId = readVarInt(cellBuffer);
  }
}
