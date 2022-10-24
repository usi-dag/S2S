package ch.usi.inf.dag.s2s.planner.qp.operators;

import ch.usi.inf.dag.s2s.planner.qp.expressions.Expression;

public class Sort extends SingleChildOperator {

    final Expression[] expressions;
    final Direction[] directions;

    public enum Direction  { ASC, DESC }

    public Sort(Expression[] expressions, Direction[] directions, Operator child) {
        super(child);
        if(expressions.length != directions.length) {
            throw new IllegalArgumentException("Different lengths (expr/dir): " + expressions.length + " " + directions.length);
        }
        this.expressions = expressions;
        this.directions = directions;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public Expression[] getExpressions() {
        return expressions;
    }

    public Direction[] getDirections() {
        return directions;
    }

}
