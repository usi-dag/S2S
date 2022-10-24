package ch.usi.inf.dag.s2s.planner.qp.operators;

import ch.usi.inf.dag.s2s.planner.qp.Field;
import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.planner.qp.expressions.Expression;

public class Project extends SingleChildOperator {

    final Expression[] projections;
    final Schema schema;

    public Project(Expression[] projections, Operator child, String[] projectionNames) {
        super(child);
        this.projections = projections;
        Field[] fields = new Field[projections.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new Field(projectionNames[i], projections[i].type());
        }
        this.schema = Schema.byFields(fields);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    public Expression[] getProjections() {
        return projections;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
