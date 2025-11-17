package processor.lexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlLexer {

  private static final Map<String, TokenType> KEYWORDS = new HashMap<>();

  static {
    KEYWORDS.put("SELECT", TokenType.KEYWORD_SELECT);
    KEYWORDS.put("FROM", TokenType.KEYWORD_FROM);
    KEYWORDS.put("WHERE", TokenType.KEYWORD_WHERE);
    KEYWORDS.put("AS", TokenType.KEYWORD_AS);
    KEYWORDS.put("AND", TokenType.KEYWORD_AND);
    KEYWORDS.put("OR", TokenType.KEYWORD_OR);
    KEYWORDS.put("NOT", TokenType.KEYWORD_NOT);
    KEYWORDS.put("IS", TokenType.KEYWORD_IS);
    KEYWORDS.put("NULL", TokenType.KEYWORD_NULL);
    KEYWORDS.put("ORDER", TokenType.KEYWORD_ORDER);
    KEYWORDS.put("LIMIT", TokenType.KEYWORD_LIMIT);
    KEYWORDS.put("OFFSET", TokenType.KEYWORD_OFFSET);
    KEYWORDS.put("BY", TokenType.KEYWORD_BY);
    KEYWORDS.put("ASC", TokenType.KEYWORD_ASC);
    KEYWORDS.put("DESC", TokenType.KEYWORD_DESC);
  }

  private final String s;
  private int i = 0;

  public SqlLexer(String s) {
    this.s = s;
  }

  private char peek() {
    return i < s.length() ? s.charAt(i) : '\0';
  }

  private char next() {
    return i < s.length() ? s.charAt(i++) : '\0';
  }

  private void skipWhitespaces() {
    while (Character.isWhitespace(peek())) {
      next();
    }
  }

  public List<Token> tokenize() {
    List<Token> out = new ArrayList<>();
    while (true) {
      skipWhitespaces();
      int pos = i;
      char c = peek();
      if (c == '\0') {
        out.add(new Token(TokenType.EOF, "", pos));
        break;
      }
      if (c == ',') {
        next();
        out.add(new Token(TokenType.COMMA, ",", pos));
        continue;
      }
      if (c == '*') {
        next();
        out.add(new Token(TokenType.STAR, "*", pos));
        continue;
      }
      if (c == '(') {
        next();
        out.add(new Token(TokenType.LPAREN, "(", pos));
        continue;
      }
      if (c == ')') {
        next();
        out.add(new Token(TokenType.RPAREN, ")", pos));
        continue;
      }
      if (c == ';') {
        next();
        out.add(new Token(TokenType.SEMICOLON, ";", pos));
        continue;
      }
      if (c == '+') {
        next();
        out.add(new Token(TokenType.PLUS, "+", pos));
        continue;
      }
      if (c == '-') {
        next();
        out.add(new Token(TokenType.MINUS, "-", pos));
        continue;
      }
      if (c == '*') {
        next();
        out.add(new Token(TokenType.MUL, "*", pos));
        continue;
      }
      if (c == '/') {
        next();
        out.add(new Token(TokenType.DIV, "/", pos));
        continue;
      }
      if (c == '=') {
        next();
        out.add(new Token(TokenType.EQ, "=", pos));
        continue;
      }
      if (c == '<') {
        next();
        if (peek() == '=') {
          next();
          out.add(new Token(TokenType.LTE, "<=", pos));
        } else if (peek() == '>') {
          next();
          out.add(new Token(TokenType.NEQ, "<>", pos));
        } else {
          out.add(new Token(TokenType.LT, "<", pos));
        }
        continue;
      }
      if (c == '>') {
        next();
        if (peek() == '=') {
          next();
          out.add(new Token(TokenType.GTE, ">=", pos));
        } else {
          out.add(new Token(TokenType.GT, ">", pos));
        }
        continue;
      }
      if (c == '\'' || c == '"') {
        char q = next();
        StringBuilder sb = new StringBuilder();
        while (peek() != '\0' && peek() != q) {
          if (peek() == '\\') {
            next();
            if (peek() != '\0') {
              sb.append(next());
            }
          } else {
            sb.append(next());
          }
        }
        if (peek() == q) {
          next();
        }
        out.add(new Token(TokenType.STRING, sb.toString(), pos));
        continue;
      }
      if (Character.isDigit(c)) {
        StringBuilder sb = new StringBuilder();
        while (Character.isDigit(peek())) {
          sb.append(next());
        }
        if (peek() == '.') {
          sb.append(next());
          while (Character.isDigit(peek())) {
            sb.append(next());
          }
        }
        out.add(new Token(TokenType.NUMBER, sb.toString(), pos));
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        StringBuilder sb = new StringBuilder();
        while (Character.isLetterOrDigit(peek()) || peek() == '_') {
          sb.append(next());
        }
        String w = sb.toString();
        TokenType kt = KEYWORDS.get(w.toUpperCase());
        if (kt != null) {
          out.add(new Token(kt, w, pos));
        } else {
          out.add(new Token(TokenType.IDENT, w, pos));
        }
        continue;
      }
      throw new RuntimeException("Unknown char at " + pos + ": " + c);
    }
    return out;
  }
}