package query_processor.parser;

import java.util.List;

interface Expression {

}

record Identifier(String name) implements Expression {

}

record Literal(Object value) implements Expression {

}

record FunctionCall(String name, List<Expression> args) implements Expression {

}

record UnaryOp(String op, Expression expr) implements Expression {

}

record BinaryOp(String op, Expression left, Expression right) implements Expression {

}

interface Statement {

}

record SelectStmt(List<SelectItem> selectList, FromItem from, Expression where,
                  List<OrderItem> orderBy, Integer limit, Integer offset) implements Statement {

}

record SelectItem(Expression expr, String alias) {

}

interface FromItem {

}

record TableRef(String name, String alias) implements FromItem {

}

record SubqueryRef(SelectStmt sub, String alias) implements FromItem {

}

record OrderItem(Expression expr, boolean asc) {

}
