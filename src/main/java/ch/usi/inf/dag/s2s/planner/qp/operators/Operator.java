package ch.usi.inf.dag.s2s.planner.qp.operators;

import ch.usi.inf.dag.s2s.planner.qp.Schema;

public interface Operator {

    void accept(OperatorVisitor visitor);

    Schema getSchema();
}
