
// This example demonstrates the use of productions in PARSE code
//(only supported in the tomita version of PARSE).
PATTERN ws := [' ','\t'];
TOKEN number := PATTERN('[0-9]+');
TOKEN plus := '+';
TOKEN minus := '-';
attrRec := RECORD
 INTEGER val;
END;
RULE(attrRec) e0 :=
                     '(' USE(attrRec,expr)? ')'
                   |  number                  TRANSFORM(attrRec, SELF.val := (INTEGER)$1;)
                   | '-' SELF                 TRANSFORM(attrRec, SELF.val := -$2.val;);
RULE(attrRec) e1 :=
                   e0
                   | SELF '*' e0              TRANSFORM(attrRec, SELF.val := $1.val * $3.val;)
                   | SELF '/' e0              TRANSFORM(attrRec, SELF.val := $1.val / $3.val;);
RULE(attrRec) e2 :=
                   e1
                   | SELF plus e1             TRANSFORM(attrRec, SELF.val := $1.val + $3.val;)
                   | SELF minus e1            TRANSFORM(attrRec, SELF.val := $1.val - $3.val;);

RULE(attrRec) expr := e2;

infile := DATASET([{'1+2*3'},{'1+2*100'},{'1+2+(3+4)*4/2'},{'-4*5'}], { STRING line });
resultsRec := RECORD
    RECORDOF(infile);
    attrRec;
    STRING exprText;
    INTEGER value3;
END;

resultsRec extractResults(infile L, attrRec attr) := TRANSFORM
   SELF := L;
   SELF := attr;
   SELF.exprText := MATCHTEXT;
   SELF.value3 := MATCHROW(e0[3]).val;
END;
OUTPUT(PARSE(infile,line,expr,extractResults(LEFT, $1),FIRST,WHOLE,PARSE,SKIP(ws)));
