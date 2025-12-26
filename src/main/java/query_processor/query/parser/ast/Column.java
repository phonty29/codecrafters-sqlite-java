package query_processor.query.parser.ast;

import java.util.List;

public record Column(
    String name,
    ColumnType type,
    List<Constraint> constraints
) {

}