package qprocessor.compiler.parser.ast;

public record CreateIndexStmt(
    String index,
    String tableName,
    String column
) implements Statement {

}
