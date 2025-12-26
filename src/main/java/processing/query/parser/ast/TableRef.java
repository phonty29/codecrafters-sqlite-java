package processing.query.parser.ast;

public record TableRef(String name, String alias) implements FromItem {

}
