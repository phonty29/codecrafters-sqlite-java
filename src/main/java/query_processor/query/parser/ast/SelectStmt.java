package query_processor.query.parser.ast;

import java.util.List;

public record SelectStmt(List<SelectItem> list, FromItem from, Expression where,
                         List<OrderItem> orderBy, Integer limit, Integer offset) implements
    Statement {

}
