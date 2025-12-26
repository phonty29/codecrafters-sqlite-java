package query_processor.query.parser.ast;

public record TableRef(String name, String alias) implements FromItem {

}
