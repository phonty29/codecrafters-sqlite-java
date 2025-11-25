package struct;

import java.nio.ByteBuffer;

public class BTreePage {
  private final PageHeader pageHeader;
  private final Cell[] cells;

  public BTreePage(ByteBuffer pageBuffer) {
    // Get the b-tree page type
    var bTreePageType = BTreePageType.valueOf(pageBuffer.get());
    // Get cells count (skip 2 bytes)
    var cellsCount = pageBuffer.position(pageBuffer.position() + 2).getShort();
    if (!bTreePageType.equals(BTreePageType.LEAF_TABLE)) {
      throw new IllegalArgumentException("Not supported page type: " + bTreePageType);
    }
    // Initialize page header
    this.pageHeader = new PageHeader(bTreePageType, cellsCount);
    // Initialize cells and fulfill
    this.cells = new Cell[cellsCount];
    // Skip 3 bytes
    pageBuffer.position(pageBuffer.position() + 3);
    ByteBuffer cellPointersBuffer = pageBuffer.slice(pageBuffer.position(), 2 * cellsCount);
    short[] offsets = new short[cellsCount];
    for (int i = 0; i < cellsCount; i++) {
      // The last table offset goes the first
      offsets[i] = cellPointersBuffer.getShort();
      ByteBuffer cellBuffer;
      if (i == 0) {
        cellBuffer = pageBuffer.position(offsets[i]).slice();
      } else {
        cellBuffer = pageBuffer.slice(offsets[i], offsets[i - 1] - offsets[i]);
      }
      // Cell
      cells[i] = new Cell(cellBuffer);
    }
  }

  public PageHeader getPageHeader() {
    return this.pageHeader;
  }

  public Cell[] getCells() {
    return this.cells;
  }
}
