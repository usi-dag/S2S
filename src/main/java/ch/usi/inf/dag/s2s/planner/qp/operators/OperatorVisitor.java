package ch.usi.inf.dag.s2s.planner.qp.operators;

public interface OperatorVisitor {
    void visit(ArrayTableScan scan);
    void visit(Project project);
    void visit(Predicate predicate);
    void visit(Aggregate aggregate);
    void visit(Sort sort);
    void visit(Limit limit);
    void visit(HashJoin join);
    void visit(NestedLoopJoin join);
}
