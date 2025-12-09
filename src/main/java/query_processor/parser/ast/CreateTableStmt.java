package query_processor.parser.ast;

import java.util.List;

public record CreateTableStmt(
    String tableName,
    List<Column> columns
) implements Statement {

}