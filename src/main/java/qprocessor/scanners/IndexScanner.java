package qprocessor.scanners;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import storage.btree.BTreePage;
import storage.cells.IndexCell;
import storage.cells.InteriorIndexCell;
import storage.cells.LeafIndexCell;
import storage.db.DatabaseProducer;
import storage.struct.Index;
import utils.ByteUtils;

public class IndexScanner {

  private final Index index;
  private final String value;

  public IndexScanner(Index index, String value) {
    this.index = index;
    this.value = value;
  }

  public List<Integer> scan() {
    if (Objects.isNull(this.index.getRootPage()) || Objects.isNull(this.index.getCurrentPage())) {
      DatabaseProducer.get().navigateTo(this.index);
    }
    return switch (index.getCurrentPage().getPageHeader().pageType()) {
      case INT_INDEX -> scanInterior();
      case LEAF_INDEX -> scanLeaf();
      default -> throw new IllegalStateException(
          "Unexpected page type for index: " + index.getRootPage().getPageHeader().pageType());
    };
  }

  public List<Integer> scanInterior() {
    if (!(index.getCurrentPage().getCells() instanceof InteriorIndexCell[] interiorCells)) {
      throw new IllegalStateException("Current page is not an interior index page");
    }
    BTreePage parentPage = index.getCurrentPage();
    List<Integer> rowIds = new ArrayList<>();
    for (InteriorIndexCell cell : interiorCells) {
      var row = convertToRow(cell);
      if (filter(row)) {
        rowIds.add(row.rowId);
      }
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), this.index);
      rowIds.addAll(this.scan());
    }
    DatabaseProducer.get().navigateToPageOfElement(parentPage.getRightmostPointer(), this.index);
    rowIds.addAll(this.scan());
    return rowIds;
  }

  private Row convertToRow(IndexCell cell) {
    String value = new String(cell.getRecordBody().values()[0]);
    int rowId = ByteUtils.toNumber(cell.getRecordBody().values()[1]).intValue();
    return new Row(rowId, value);
  }

  public List<Integer> scanLeaf() {
    if (!(index.getCurrentPage().getCells() instanceof LeafIndexCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf index page");
    }
    return Arrays
        .stream(leafCells)
        .map(this::convertToRow)
        .filter(this::filter)
        .map(Row::rowId)
        .toList();
  }

  private boolean filter(Row row) {
    return row.value.contentEquals(this.value);
  }

  public record Row(
      int rowId,
      String value
  ) {

  }
}
