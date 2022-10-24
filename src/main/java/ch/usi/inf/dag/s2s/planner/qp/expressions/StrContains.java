package ch.usi.inf.dag.s2s.planner.qp.expressions;

public class StrContains extends BooleanExpression {
    // represents a LIKE operator in the form X LIKE %a%

    final String pattern;
    final Expression stringGetter;

    public StrContains(String pattern, Expression stringGetter) {
        this.pattern = pattern;
        this.stringGetter = stringGetter;
        if(stringGetter.type() != String.class) {
            throw new IllegalArgumentException(this.getClass() + " requires a string returning subexpression");
        }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public String getPattern() {
        return pattern;
    }

    public Expression getStringGetter() {
        return stringGetter;
    }
}
