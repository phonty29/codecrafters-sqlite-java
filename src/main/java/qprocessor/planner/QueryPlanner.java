package qprocessor.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import qprocessor.Row;
import qprocessor.compiler.QueryCompiler;
import qprocessor.compiler.parser.ast.BinaryOp;
import qprocessor.compiler.parser.ast.SelectStmt;
import qprocessor.scanners.IndexScanner;
import storage.db.DatabaseProducer;

public class QueryPlanner {
  private ScanType scanType = ScanType.TABLE_SCAN;
  private final List<IndexScanner> indexScanners = new ArrayList<>();
  private Function<Row, Boolean> filter = (_) -> true;

  public QueryPlanner(SelectStmt select) {
    Map<String, Object> keyValues;
    if (Objects.nonNull(select) && Objects.nonNull(select.where()) && select.where() instanceof BinaryOp) {
      keyValues = ((BinaryOp) select.where()).keyValues();
    } else {
      keyValues = new HashMap<>();
    }

    DatabaseProducer.get().getIndexes().forEach(idx -> {
      String column = new QueryCompiler(idx.meta().createStmt()).createIndex().column();
      if (keyValues.containsKey(column)) {
        this.scanType = ScanType.INDEX_SCAN;
        this.indexScanners.add(idx.scanner(keyValues.get(column).toString()));
      }
    });

    if (Objects.nonNull(select.where()) && select.where() instanceof BinaryOp
        && this.scanType.equals(ScanType.TABLE_SCAN)) {
      this.filter = (r) -> select.where().eval(r);
    }
  }

  public Function<Row, Boolean> filter() {
    return this.filter;
  }

  public ScanType scanType() {
    return this.scanType;
  }

  public List<IndexScanner> indexScanners() {
    return this.indexScanners;
  }
}
