package query_processor.query.parser.ast;

public record SubqueryRef(SelectStmt sub, String alias) implements FromItem {

}
