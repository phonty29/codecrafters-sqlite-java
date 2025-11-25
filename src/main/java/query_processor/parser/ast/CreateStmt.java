package query_processor.parser.ast;

import java.util.List;

public record CreateStmt(
    String tableName,
    List<Column> columns
) implements Statement {}