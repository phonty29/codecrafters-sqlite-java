package processor.parser.ast;

public record SubqueryRef(SelectStmt sub, String alias) implements FromItem {

}
