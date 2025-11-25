package query_processor.parser.ast;

public record Constraint(
    String value,
    Expression exp
) {

}