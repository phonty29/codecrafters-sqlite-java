package query_processor.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import query_processor.lexer.Token;
import query_processor.lexer.TokenType;
import query_processor.parser.ast.BinaryOp;
import query_processor.parser.ast.Column;
import query_processor.parser.ast.ColumnType;
import query_processor.parser.ast.Constraint;
import query_processor.parser.ast.CreateStmt;
import query_processor.parser.ast.Expression;
import query_processor.parser.ast.FromItem;
import query_processor.parser.ast.FunctionCall;
import query_processor.parser.ast.Identifier;
import query_processor.parser.ast.Literal;
import query_processor.parser.ast.OrderItem;
import query_processor.parser.ast.SelectItem;
import query_processor.parser.ast.SelectStmt;
import query_processor.parser.ast.SubqueryRef;
import query_processor.parser.ast.TableRef;
import query_processor.parser.ast.UnaryOp;

public class SqlParser {

  // ----- Pratt expression parser -----
  // precedence numbers (lower = tighter binding)
  private static final Map<TokenType, Integer> INFIX_PRECEDENCE = new HashMap<>();

  static {
    INFIX_PRECEDENCE.put(TokenType.MUL, 70);
    INFIX_PRECEDENCE.put(TokenType.DIV, 70);
    INFIX_PRECEDENCE.put(TokenType.PLUS, 60);
    INFIX_PRECEDENCE.put(TokenType.MINUS, 60);
    INFIX_PRECEDENCE.put(TokenType.EQ, 50);
    INFIX_PRECEDENCE.put(TokenType.NEQ, 50);
    INFIX_PRECEDENCE.put(TokenType.LT, 50);
    INFIX_PRECEDENCE.put(TokenType.LTE, 50);
    INFIX_PRECEDENCE.put(TokenType.GT, 50);
    INFIX_PRECEDENCE.put(TokenType.GTE, 50);
    INFIX_PRECEDENCE.put(TokenType.KEYWORD_AND, 30);
    INFIX_PRECEDENCE.put(TokenType.KEYWORD_OR, 20);
  }

  private final List<Token> tokens;
  private int p = 0;

  public SqlParser(List<Token> tokens) {
    this.tokens = tokens;
  }

  private Token t() {
    return tokens.get(p);
  }

  private boolean match(TokenType... types) {
    for (TokenType ty : types) {
      if (t().type == ty) {
        p++;
        return true;
      }
    }
    return false;
  }

  private Token consume(TokenType ty, String err) {
    if (t().type == ty) {
      return tokens.get(p++);
    }
    throw new RuntimeException("Parse error at " + t().pos + ": " + err + " got " + t());
  }

  public CreateStmt parseCreateStmt() {
    consume(TokenType.KEYWORD_CREATE, "expected CREATE");
    consume(TokenType.KEYWORD_TABLE, "expected TABLE after CREATE");
    Token name = consume(TokenType.IDENT, "expected table name");
    List<Column> columns = parseColumns();
    return new CreateStmt(name.text, columns);
  }

  private List<Column> parseColumns() {
    consume(TokenType.LPAREN, "expected ( after CREATE TABLE [tableName]");
    List<Column> columns = new ArrayList<>();
    do {
      Token columnName = consume(TokenType.IDENT, "Expected IDENT [columnName]");
      ColumnType columnType = parseColumnType();
      List<Constraint> constraints = parseConstraints();
      columns.add(new Column(columnName.text, columnType, constraints));
    } while (match(TokenType.COMMA));
    return columns;
  }

  private boolean isConstraint(TokenType tt) {
    return tt == TokenType.KEYWORD_PRIMARY
        || tt == TokenType.KEYWORD_NOT
        || tt == TokenType.KEYWORD_UNIQUE
        || tt == TokenType.KEYWORD_DEFAULT
        || tt == TokenType.KEYWORD_AUTOINCREMENT;
  }

  private List<Constraint> parseConstraints() {
    List<Constraint> constraints = new ArrayList<>();
    while (isConstraint(t().type)) {
      if (match(TokenType.KEYWORD_PRIMARY)) {
        consume(TokenType.KEYWORD_KEY, "Expected KEY after PRIMARY");
        constraints.add(new Constraint("PRIMARY KEY", null));
        continue;
      }
      if (match(TokenType.KEYWORD_NOT)) {
        consume(TokenType.KEYWORD_NULL, "Expected NULL after NOT");
        constraints.add(new Constraint("NOT NULL", null));
        continue;
      }
      if (match(TokenType.KEYWORD_DEFAULT)) {
        Expression expr = parseExpression(0);
        constraints.add(new Constraint("DEFAULT", expr));
        continue;
      }
      if (match(TokenType.KEYWORD_AUTOINCREMENT)) {
        constraints.add(new Constraint("AUTOINCREMENT", null));
        continue;
      }
      // … other constraints …
      throw new IllegalStateException("Unsupported constraint at " + t());
    }
    return constraints;
  }

  private ColumnType parseColumnType() {
    if (match(TokenType.TYPE_INTEGER)) {
      return ColumnType.INTEGER;
    } else if (match(TokenType.TYPE_TEXT)) {
      return ColumnType.TEXT;
    } else if (match(TokenType.TYPE_REAL)) {
      return ColumnType.REAL;
    } else if (match(TokenType.KEYWORD_NULL)) {
      return ColumnType.NULL;
    } else {
      return ColumnType.INTEGER;
    }
  }

  public SelectStmt parseSelect() {
    consume(TokenType.KEYWORD_SELECT, "expected SELECT");
    List<SelectItem> select = parseSelectList();
    consume(TokenType.KEYWORD_FROM, "expected FROM");
    FromItem from = parseFromItem();
    Expression where = null;
    if (match(TokenType.KEYWORD_WHERE)) {
      where = parseExpression(0);
    }
    List<OrderItem> order = null;
    if (match(TokenType.KEYWORD_ORDER)) {
      consume(TokenType.KEYWORD_BY, "expected BY after ORDER");
      order = parseOrderBy();
    }
    Integer limit = null;
    Integer offset = null;
    if (match(TokenType.KEYWORD_LIMIT)) {
      Token num = consume(TokenType.NUMBER, "expected limit number");
      limit = Integer.parseInt(num.text);
    }
    if (match(TokenType.KEYWORD_OFFSET)) {
      Token num = consume(TokenType.NUMBER, "expected offset number");
      offset = Integer.parseInt(num.text);
    }
    match(TokenType.SEMICOLON);
    return new SelectStmt(select, from, where, order, limit, offset);
  }

  private List<SelectItem> parseSelectList() {
    List<SelectItem> out = new ArrayList<>();
    if (match(TokenType.STAR)) {
      out.add(new SelectItem(new Identifier("*"), null));
      return out;
    }
    do {
      Expression e = parseExpression(0);
      String alias = null;
      if (match(TokenType.KEYWORD_AS)) {
        Token id = consume(TokenType.IDENT, "expected alias");
        alias = id.text;
      } else if (t().type == TokenType.IDENT) { // allow implicit alias
        Token maybe = tokens.get(p);
        // lookahead: next token is comma/ FROM / WHERE etc -> treat as alias
        Token next = tokens.get(p + 1);
        if (next.type == TokenType.COMMA || next.type == TokenType.KEYWORD_FROM
            || next.type == TokenType.KEYWORD_WHERE
            || next.type == TokenType.SEMICOLON || next.type == TokenType.EOF) {
          alias = maybe.text;
          p++;
        }
      }
      out.add(new SelectItem(e, alias));
    } while (match(TokenType.COMMA));
    return out;
  }

  private FromItem parseFromItem() {
    if (match(TokenType.LPAREN)) {
      SelectStmt sub = parseSelect();
      consume(TokenType.RPAREN, "expected ) after subquery");
      String alias = null;
      if (match(TokenType.KEYWORD_AS)) {
        alias = consume(TokenType.IDENT, "alias").text;
      } else if (t().type == TokenType.IDENT) {
        alias = t().text;
        p++;
      }
      return new SubqueryRef(sub, alias);
    } else {
      Token name = consume(TokenType.IDENT, "expected table name");
      String alias = null;
      if (match(TokenType.KEYWORD_AS)) {
        alias = consume(TokenType.IDENT, "expected alias").text;
      } else if (t().type == TokenType.IDENT) {
        alias = t().text;
        p++;
      }
      return new TableRef(name.text, alias);
    }
  }

  private Expression parseExpression(int minPrec) {
    Expression left = parsePrefix();
    while (true) {
      TokenType tt = t().type;
      Integer prec = INFIX_PRECEDENCE.get(tt);
      if (prec == null || prec <= minPrec) {
        break;
      }
      Token op = t();
      p++;
      // special handling for IN (list) and function calls handled in prefix if needed
      Expression right = parseExpression(prec);
      left = new BinaryOp(op.text, left, right);
    }
    return left;
  }

  private Expression parsePrefix() {
    Token cur = t();
    if (match(TokenType.NUMBER)) {
      return new Literal(Long.parseLong(cur.text));
    }
    if (match(TokenType.STRING)) {
      return new Literal(cur.text);
    }
    if (match(TokenType.IDENT)) {
      // function call or identifier
      if (t().type == TokenType.LPAREN) {
        // function call
        String fname = cur.text;
        p++; // consume '('
        List<Expression> args = new ArrayList<>();
        if (!match(TokenType.RPAREN)) {
          do {
            args.add(parseExpression(0));
          } while (match(TokenType.COMMA));
          consume(TokenType.RPAREN, "expected )");
        }
        return new FunctionCall(fname, args);
      } else {
        return new Identifier(cur.text);
      }
    }
    if (match(TokenType.LPAREN)) {
      Expression e = parseExpression(0);
      consume(TokenType.RPAREN, "expected )");
      return e;
    }
    if (match(TokenType.PLUS) || match(TokenType.MINUS) || match(TokenType.KEYWORD_NOT)) {
      String op = cur.text;
      Expression r = parseExpression(80);
      return new UnaryOp(op, r);
    }
    if (match(TokenType.STAR)) {
      return new Identifier("*");
    }
    throw new RuntimeException("Unexpected token in expression at " + cur.pos + ": " + cur);
  }


  private List<OrderItem> parseOrderBy() {
    List<OrderItem> out = new ArrayList<>();
    do {
      Expression e = parseExpression(0);
      boolean asc = !match(TokenType.KEYWORD_DESC);
      out.add(new OrderItem(e, asc));
    } while (match(TokenType.COMMA));
    return out;
  }
}