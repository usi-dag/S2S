package ch.usi.inf.dag.s2s.planner.qp.operators;

import ch.usi.inf.dag.s2s.planner.qp.expressions.Expression;

public interface Join extends Operator {
    enum JoinType {
        /**
         * Inner join.
         */
        INNER,

        /**
         * Left-outer join.
         */
        LEFT,

        /**
         * Right-outer join.
         */
        RIGHT,

        /**
         * Full-outer join.
         */
        FULL,

        /**
         * Semi-join.
         *
         * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
         * that have a corresponding {@code DEPT} record:
         *
         * <blockquote><pre>
         * SELECT * FROM EMP
         * WHERE EXISTS (SELECT 1 FROM DEPT
         *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
         * </blockquote>
         */
        SEMI,

        /**
         * Anti-join (also known as Anti-semi-join).
         *
         * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
         * that do not have a corresponding {@code DEPT} record:
         *
         * <blockquote><pre>
         * SELECT * FROM EMP
         * WHERE NOT EXISTS (SELECT 1 FROM DEPT
         *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
         * </blockquote>
         */
        ANTI
    }
    Operator left();
    Operator right();

    Expression[] mapper();

    Expression nonEquiCondition();

    boolean isLeftProjectMapper();
    boolean isRightProjectMapper();

    JoinType joinType();

}
