grammar MooDB;

evaluation
    : ID? predicate?
    ;


predicate
    : '[' expr ']'
    ;

expr
    : l=expr o=('='|'>'|'<'|'>='|'<=') r=expr #evalExpr
    | l=expr 'and' r=expr #andExpr
    | l=expr 'or' r=expr #orExpr
    | '(' expr ')' #expr1
    | ID # exprId
    | STRINGLITERAL #exprStrLit
    ;


STRINGLITERAL
    : '\'' ('\\\'' | ~('\''))+ '\''
    ;

WS
    : [ \t\r\n]+ -> skip
    ;

ID
    : '%'? ('A'..'Z' | 'a'..'z' | '0'..'9')+
    ;