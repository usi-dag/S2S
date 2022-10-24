package ch.usi.inf.dag.s2s.planner.qp.operators;

import ch.usi.inf.dag.s2s.planner.qp.Schema;

public abstract class SingleChildOperator implements Operator {

    final Operator child;


    protected SingleChildOperator(Operator child) {
        this.child = child;
    }


    public final Operator getChild() {
        return child;
    }

    @Override
    public Schema getSchema() {
        return child.getSchema();
    }

}
