package ch.usi.inf.dag.s2s.planner.qp.expressions;


public interface Expression {
    <T> T accept(ExpressionVisitor<T> visitor);

    Class<?> type();
}
