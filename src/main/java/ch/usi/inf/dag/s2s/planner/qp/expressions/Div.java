package ch.usi.inf.dag.s2s.planner.qp.expressions;

import ch.usi.inf.dag.s2s.planner.qp.PlanningException;

public class Div extends BinaryExpression {
    public Div(Expression left, Expression right) throws PlanningException {
        super(left, right, double.class);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}