package query_processor.query.parser.ast;

public record Constraint(
    String value,
    Expression exp
) {

}