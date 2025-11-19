package processor.lexer;

public enum TokenType {
  IDENT,
  NUMBER,
  STRING,
  STAR,
  COMMA,
  DOT,
  LPAREN, // left parentheses
  RPAREN, // right parentheses
  SEMICOLON,
  PLUS,
  MINUS,
  MUL,
  DIV,
  EQ, // equals
  NEQ, // not equals
  LT, // less than
  LTE, // less than and equals
  GT, // greater than
  GTE,  // greate than equals
  KEYWORD_SELECT,
  KEYWORD_FROM,
  KEYWORD_WHERE,
  KEYWORD_AS,
  KEYWORD_AND,
  KEYWORD_OR,
  KEYWORD_NOT,
  KEYWORD_IS,
  KEYWORD_NULL,
  KEYWORD_ORDER,
  KEYWORD_BY,
  KEYWORD_LIMIT,
  KEYWORD_OFFSET,
  KEYWORD_ASC,
  KEYWORD_DESC,
  KEYWORD_CREATE,
  KEYWORD_TABLE,
  KEYWORD_PRIMARY,
  KEYWORD_KEY,
  KEYWORD_DEFAULT,
  KEYWORD_UNIQUE,
  KEYWORD_AUTOINCREMENT,
  TYPE_INTEGER,
  TYPE_TEXT,
  TYPE_REAL,
  EOF
}