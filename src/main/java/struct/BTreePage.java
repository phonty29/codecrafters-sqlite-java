package struct;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BTreePage {

  private final PageHeader pageHeader;
  private final Cell[] cells;

  public BTreePage(ByteBuffer pageBuffer) {
    // Get the b-tree page type
    var bTreePageType = BTreePageType.valueOf(pageBuffer.get());
    // Get leafTableCells count (skip 2 bytes)
    var cellsCount = pageBuffer.position(pageBuffer.position() + 2).getShort();
    // Initialize page header
    this.pageHeader = new PageHeader(bTreePageType, cellsCount);
    // Initialize leafTableCells and fulfill
    this.cells = new Cell[cellsCount];
    short startOfTheCellContentArea = pageBuffer.getShort();
    // Skip 1 byte
    pageBuffer.position(pageBuffer.position() + 1);
    // Start reading the cell pointers array
    ByteBuffer cellPointersBuffer = pageBuffer.slice(pageBuffer.position(), 2 * cellsCount);
    short[] offsets = new short[cellsCount];
    for (int i = 0; i < cellsCount; i++) {
      // The last table offset goes the first
      offsets[i] = cellPointersBuffer.getShort();
      ByteBuffer cellBuffer;
      if (startOfTheCellContentArea == offsets[0]) {
        if (cellsCount == 1) {
          cellBuffer = pageBuffer.position(offsets[i]).slice();
          cells[i] = initCell(cellBuffer);
        }
        if (i > 0) {
          cellBuffer = pageBuffer.slice(offsets[i-1], offsets[i] - offsets[i-1]);
          cells[i-1] = initCell(cellBuffer);
          if (i == cellsCount - 1) {
            cellBuffer = pageBuffer.position(offsets[i]).slice();
            cells[i] = initCell(cellBuffer);
          }
        }
      } else {
        if (i == 0) {
          cellBuffer = pageBuffer.position(offsets[i]).slice();
        } else {
          cellBuffer = pageBuffer.slice(offsets[i], offsets[i - 1] - offsets[i]);
        }
        cells[i] = initCell(cellBuffer);
      }
    }
  }

  private Cell initCell(ByteBuffer cellBuffer) {
    if (this.pageHeader.pageType.equals(BTreePageType.LEAF_TABLE)) {
      return new LeafTableCell(cellBuffer);
    } else if (this.pageHeader.pageType.equals(BTreePageType.INT_TABLE)) {
      return new InteriorTableCell(cellBuffer);
    }
    throw new IllegalStateException("Unsupported page type: " + this.pageHeader.pageType);
  }

  public PageHeader getPageHeader() {
    return this.pageHeader;
  }

  public LeafTableCell[] getCells() {
    return Arrays.stream(this.cells).map(cell -> (LeafTableCell) cell).toArray(LeafTableCell[]::new);
  }

  public record PageHeader(BTreePageType pageType, int cellsCount) {

  }
}
