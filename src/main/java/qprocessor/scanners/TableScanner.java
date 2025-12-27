package qprocessor.scanners;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import qprocessor.Row;
import qprocessor.compiler.QueryCompiler;
import qprocessor.compiler.parser.ast.Column;
import qprocessor.compiler.parser.ast.ColumnType;
import qprocessor.planner.QueryPlanner;
import storage.btree.BTreePage;
import storage.cells.InteriorTableCell;
import storage.cells.LeafTableCell;
import storage.db.DatabaseProducer;
import storage.struct.Table;
import utils.ByteUtils;

public class TableScanner {

  private final Table table;
  private final List<Column> columns;

  public TableScanner(Table table) {
    this.table = table;
    this.columns = new QueryCompiler(table.meta().sqlStmt()).createTable().columns();
  }

  public List<String> scan(List<String> queriedColumns, QueryPlanner planner) {
    return switch (planner.scanType()) {
      case INDEX_SCAN -> {
        List<Integer> rowIds = planner.indexScanners().stream().map(IndexScanner::scan)
            .flatMap(List::stream).toList();
        yield this.scanWithIndex(queriedColumns, rowIds);
      }
      case TABLE_SCAN -> switch (table.getRootPage().getPageHeader().pageType()) {
        case LEAF_TABLE -> scanLeaf(queriedColumns, planner.filter());
        case INT_TABLE -> scanInterior(queriedColumns, planner.filter());
        default -> throw new IllegalStateException(
            "Not supported page type: " + table.getCurrentPage().getPageHeader().pageType());
      };
    };
  }

  public List<String> scanWithIndex(List<String> queriedColumns, List<Integer> rowIds) {
    List<String> values = new ArrayList<>();
    BTreePage parentPage = table.getCurrentPage();
    rowIds.forEach(rowId -> {
      table.setCurrentPage(parentPage);
      values.add(this.scanByRowId(rowId, queriedColumns));
    });
    return values;
  }

  public String scanByRowId(Integer rowId, List<String> queriedColumns) {
    return switch (table.getCurrentPage().getPageHeader().pageType()) {
      case INT_TABLE -> scanInteriorByRowId(rowId, queriedColumns);
      case LEAF_TABLE -> scanLeafByRowId(rowId, queriedColumns);
      default -> throw new IllegalStateException(
          "Not supported page type: " + table.getCurrentPage().getPageHeader().pageType());
    };
  }

  private String scanInteriorByRowId(Integer rowId, List<String> queriedColumns) {
    if (!(table.getCurrentPage().getCells() instanceof InteriorTableCell[] interiorTableCells)) {
      throw new IllegalStateException("Current page is not an interior table");
    }

    BTreePage parentPage = table.getCurrentPage();
    for (var cell : interiorTableCells) {
      if (rowId <= cell.getRowId()) {
        DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), table);
        return this.scanByRowId(rowId, queriedColumns);
      }
    }
    DatabaseProducer.get().navigateToPageOfElement(parentPage.getRightmostPointer(), table);
    return this.scanByRowId(rowId, queriedColumns);
  }

  private String scanLeafByRowId(Integer rowId, List<String> queriedColumns) {
    if (!(table.getCurrentPage().getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    for (var cell : leafCells) {
      if (rowId == cell.getRowId()) {
        return formatRowColumns(toRow(cell), queriedColumns);
      }
    }
    throw new IllegalStateException("Current page doesn't contain row: " + rowId);
  }

  private List<String> scanInterior(List<String> columns, Function<Row, Boolean> filter) {
    List<String> values = new ArrayList<>();
    Arrays.stream((InteriorTableCell[]) table.getRootPage().getCells()).forEach(cell -> {
      DatabaseProducer.get().navigateToPageOfElement(cell.getLeftChildPointer(), table);
      values.addAll(this.scanLeaf(columns, filter));
    });
    DatabaseProducer.get()
        .navigateToPageOfElement(table.getRootPage().getRightmostPointer(), table);
    values.addAll(this.scanLeaf(columns, filter));
    return values;
  }

  private List<String> scanLeaf(List<String> queriedColumns, Function<Row, Boolean> filter) {
    if (!(table.getCurrentPage().getCells() instanceof LeafTableCell[] leafCells)) {
      throw new IllegalStateException("Current page is not a leaf table");
    }

    return Arrays.stream(leafCells)
        .map(this::toRow)
        .filter(filter::apply)
        .map(row -> formatRowColumns(row, queriedColumns))
        .toList();
  }

  private Row toRow(LeafTableCell cell) {
    Map<String, Object> rowValue = new HashMap<>();
    for (int i = 0; i < this.columns.size(); i++) {
      Column column = this.columns.get(i);
      // Replace [id] with [rowId] if [id] is not present
      if (column.name().equals("id")
          && column.type() == ColumnType.INTEGER
          && cell.getRecordBody().values()[i].length == 0) {
        rowValue.put(column.name(), Integer.toString(cell.getRowId()));
        continue;
      }
      rowValue.put(column.name(), retrieveValueOfColumn(i, cell.getRecordBody().values()[i]));
    }
    return new Row(rowValue);
  }

  private String formatRowColumns(Row row, List<String> columns) {
    return columns
        .stream()
        .map(col -> (String) row.get(col))
        .collect(Collectors.joining("|"));
  }


  private String retrieveValueOfColumn(int col, byte[] value) {
    return switch (this.columns.get(col).type()) {
      case TEXT -> new String(value);
      case INTEGER, REAL -> Double.toString(ByteUtils.toNumber(value).doubleValue());
      case NULL -> "null";
    };
  }
}
