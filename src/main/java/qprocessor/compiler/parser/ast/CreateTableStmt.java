package qprocessor.compiler.parser.ast;

import java.util.List;

public record CreateTableStmt(
    String tableName,
    List<Column> columns
) implements Statement {

}