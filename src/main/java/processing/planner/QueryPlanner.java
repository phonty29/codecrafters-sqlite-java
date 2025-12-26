package processing.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import processing.query.QueryEngine;
import processing.query.parser.ast.BinaryOp;
import processing.query.parser.ast.SelectStmt;
import processing.scanners.IndexScanner;
import storage.db.DatabaseProducer;

public class QueryPlanner {
  private ScanType scanType = ScanType.TABLE_SCAN;
  private final List<IndexScanner> indexScanners = new ArrayList<>();

  public QueryPlanner(SelectStmt select) {
    Map<String, Object> keyValues;
    if (Objects.nonNull(select) && Objects.nonNull(select.where()) && select.where() instanceof BinaryOp) {
      keyValues = ((BinaryOp) select.where()).keyValues();
    } else {
      keyValues = new HashMap<>();
    }

    DatabaseProducer.get().getIndexes().forEach(idx -> {
      String column = new QueryEngine(idx.meta().createStmt()).createIndex().column();
      if (keyValues.containsKey(column)) {
        this.scanType = ScanType.INDEX_SCAN;
        this.indexScanners.add(new IndexScanner(idx, column, keyValues.get(column).toString()));
      }
    });
  }

  public ScanType scanType() {
    return this.scanType;
  }

  public List<IndexScanner> indexScanners() {
    return this.indexScanners;
  }
}
