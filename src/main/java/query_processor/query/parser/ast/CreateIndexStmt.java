package query_processor.query.parser.ast;

import java.util.List;

public record CreateIndexStmt(
    String index,
    String tableName,
    List<String> columns
) implements Statement {

}
