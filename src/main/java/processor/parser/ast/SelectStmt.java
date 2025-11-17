package processor.parser.ast;

import java.util.List;

public record SelectStmt(List<SelectItem> selectList, FromItem from, Expression where,
                         List<OrderItem> orderBy, Integer limit, Integer offset) implements
    Statement {

}
