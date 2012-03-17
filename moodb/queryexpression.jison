/* description: Parses end executes mathematical expressions. */

/* lexical grammar */
%lex
%%

\s+                   /* skip whitespace */
"OR"                  return 'OR'
"AND"                 return 'AND'
(("<>"|[<>]"="?)|"=")      return 'OP'
\"(\\.|[^\\"])*\"     return 'STRING'
[a-z|A-Z|0-9_]+       return 'ID'
<<EOF>>               return 'EOF'


/lex

/* operator associations and precedence */

%start expressions

%% /* language grammar */

expressions
    : e EOF
        {return $1;}
    ;

id
    : ID
         {$$ = $1;}
    | STRING
         {$$ = $1.substring(1,$1.length-1);}
    ;

e
    : id OP id ep
        {$$ = "(key = '" + $1 +"' AND value " + $2 + " '" + $3 + "')" + $4;}
    | '(' e ')'
        {$$ = $2;}
    ;

ep
    : 'AND' e {$$ = " AND " + $2;}
    | 'OR' e {$$ = " OR " + $2;}
    | {$$ = "";}
    ;

