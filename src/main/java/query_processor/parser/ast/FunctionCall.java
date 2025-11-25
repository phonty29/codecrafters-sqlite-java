package query_processor.parser.ast;

import java.util.List;

public record FunctionCall(String name, List<Expression> args) implements Expression {

}
