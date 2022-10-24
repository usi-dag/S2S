package ch.usi.inf.dag.s2s.planner.qp.operators;

import ch.usi.inf.dag.s2s.planner.qp.Field;
import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.planner.qp.expressions.Expression;
import ch.usi.inf.dag.s2s.planner.qp.expressions.InputRef;

import java.util.Arrays;

public record HashJoin(
        Operator left,
        Operator right,
        Expression[] leftKeyGetters,
        Expression[] rightKeyGetters,
        Expression[] mapper,
        Expression nonEquiCondition,
        boolean isLeftProjectMapper,
        boolean isRightProjectMapper,
        JoinType joinType,
        Schema outputSchema
) implements Join {

    public static HashJoin create(Operator left,
                                  Operator right,
                                  Expression[] leftKeyGetters,
                                  Expression[] rightKeyGetters,
                                  Expression[] mapper,
                                  JoinType joinType,
                                  Schema schema) {
        return new HashJoin(left, right, leftKeyGetters, rightKeyGetters, mapper, null, false, false, joinType, schema);
    }

    public static HashJoin create(Operator left,
                                  Operator right,
                                  Expression[] leftKeyGetters,
                                  Expression[] rightKeyGetters,
                                  InputRef[] mapper,
                                  JoinType joinType) {
        return create(left, right,
                leftKeyGetters, rightKeyGetters,
                null,
                mapper,
                joinType);
    }
    public static HashJoin create(Operator left,
                                  Operator right,
                                  Expression[] leftKeyGetters,
                                  Expression[] rightKeyGetters,
                                  Expression nonEquiCondition,
                                  InputRef[] mapper,
                                  JoinType joinType) {
        return new HashJoin(left, right,
                leftKeyGetters, rightKeyGetters,
                mapper,
                nonEquiCondition, false, false,
                joinType,
                Schema.byFields(Arrays.stream(mapper).map(InputRef::field).toArray(Field[]::new)));
    }

    public static HashJoin createInnerWithRightProjectMapper(Operator left,
                                                             Operator right,
                                                             Expression[] leftKeyGetters,
                                                             Expression[] rightKeyGetters) {
        return new HashJoin(left, right, leftKeyGetters, rightKeyGetters, null, null, false, true, JoinType.INNER, right.getSchema());
    }

    public static HashJoin createInnerWithRightProjectMapper(Operator left,
                                                             Operator right,
                                                             Expression[] leftKeyGetters,
                                                             Expression[] rightKeyGetters,
                                                             Expression nonEquiCondition) {
        return new HashJoin(left, right, leftKeyGetters, rightKeyGetters, null, nonEquiCondition, false, true, JoinType.INNER, right.getSchema());
    }

    public static HashJoin createInnerWithLeftProjectMapper(Operator left,
                                                            Operator right,
                                                            Expression[] leftKeyGetters,
                                                            Expression[] rightKeyGetters) {
        return new HashJoin(left, right, leftKeyGetters, rightKeyGetters, null, null, true, false, JoinType.INNER, left.getSchema());
    }
    public static HashJoin createInnerWithLeftProjectMapper(Operator left,
                                                            Operator right,
                                                            Expression[] leftKeyGetters,
                                                            Expression[] rightKeyGetters,
                                                            Expression nonEquiCondition) {
        return new HashJoin(left, right, leftKeyGetters, rightKeyGetters, null, nonEquiCondition, true, false, JoinType.INNER, left.getSchema());
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Schema getSchema() {
        return outputSchema;
    }


}
