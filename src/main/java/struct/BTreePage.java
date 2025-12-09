package struct;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.OptionalInt;
import struct.cells.Cell;
import struct.cells.InteriorTableCell;
import struct.cells.LeafTableCell;

public class BTreePage {

  private final PageHeader pageHeader;
  private final Cell[] cells;
  private OptionalInt rightmostPointer = OptionalInt.empty();

  public BTreePage(ByteBuffer pageBuffer) {
    // Get the b-tree page type
    var bTreePageType = BTreePageType.valueOf(pageBuffer.get());
    // Get cells count (skip 2 bytes)
    var cellsCount = pageBuffer.position(pageBuffer.position() + 2).getShort();
    // Initialize page header
    this.pageHeader = new PageHeader(bTreePageType, cellsCount);
    // Initialize cells and fulfill
    this.cells = new Cell[cellsCount];
    short startOfTheCellContentArea = pageBuffer.getShort();
    // Skip 1 byte
    pageBuffer.position(pageBuffer.position() + 1);
    // Right-most pointer
    if (bTreePageType.equals(BTreePageType.INT_TABLE)) {
      this.rightmostPointer = OptionalInt.of(pageBuffer.getInt());
    }
    // Start reading the cell pointers array
    ByteBuffer cellPointersBuffer = pageBuffer.slice(pageBuffer.position(), 2 * cellsCount);
    short[] offsets = new short[cellsCount];
    // Collect offsets from the cell pointers array
    for (int i = 0; i < cellsCount; i++) {
      offsets[i] = cellPointersBuffer.getShort();
    }
    Arrays.sort(offsets);
    // Initialize cells
    for (int i = offsets.length - 1; i >= 0; i--) {
      ByteBuffer cellBuffer;
      if (i == offsets.length - 1) {
        cellBuffer = pageBuffer.position(offsets[i]).slice();
      } else {
        cellBuffer = pageBuffer.slice(offsets[i], offsets[i + 1] - offsets[i]);
      }
      cells[i] = initCell(cellBuffer);
    }
    Arrays.sort(cells, Comparator.comparingInt(Cell::getRowId));
  }

  private Cell initCell(ByteBuffer cellBuffer) {
    return switch (this.pageHeader.pageType) {
      case BTreePageType.INT_TABLE -> new InteriorTableCell(cellBuffer);
      case BTreePageType.LEAF_TABLE -> new LeafTableCell(cellBuffer);
      default ->
          throw new IllegalStateException("Not supported page type: " + this.pageHeader.pageType);
    };
  }

  public PageHeader getPageHeader() {
    return this.pageHeader;
  }

  public Cell[] getCells() {
    return switch (this.pageHeader.pageType) {
      case LEAF_TABLE ->
          Arrays.stream(this.cells).map(cell -> (LeafTableCell) cell).toArray(LeafTableCell[]::new);
      case INT_TABLE -> Arrays.stream(this.cells).map(cell -> (InteriorTableCell) cell)
          .toArray(InteriorTableCell[]::new);
      default ->
          throw new IllegalStateException("Not supported page type: " + this.pageHeader.pageType);
    };
  }

  public int getRightmostPointer() {
    if (this.pageHeader.pageType.equals(BTreePageType.INT_TABLE)
        && this.rightmostPointer.isPresent()) {
      return this.rightmostPointer.getAsInt();
    }
    throw new IllegalStateException("Not an interior table page");
  }

  public record PageHeader(BTreePageType pageType, int cellsCount) {

  }
}
