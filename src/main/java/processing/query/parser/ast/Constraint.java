package processing.query.parser.ast;

public record Constraint(
    String value,
    Expression exp
) {

}