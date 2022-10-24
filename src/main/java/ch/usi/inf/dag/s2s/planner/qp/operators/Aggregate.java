package ch.usi.inf.dag.s2s.planner.qp.operators;


import ch.usi.inf.dag.s2s.planner.qp.Field;
import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.planner.qp.expressions.Expression;

import java.util.stream.IntStream;

public class Aggregate extends SingleChildOperator {

    final Expression[] groupKeys;
    final int[] groupSelector;
    final SqlAggFunction[] aggregations;
    final Schema outputSchema; // the output schema could be provided or inferred

    public Aggregate(Expression[] groupKeys,
                     SqlAggFunction[] aggregations,
                     int[] groupSelector,
                     String[] fieldNames,
                     Operator child) {
        super(child);
        this.groupKeys = groupKeys != null ? groupKeys : new Expression[0];
        this.aggregations = aggregations;
        this.outputSchema = inferSchema(groupSelector, groupKeys, aggregations, fieldNames);
        this.groupSelector = groupSelector;
    }

    public Aggregate(Expression[] groupKeys, SqlAggFunction[] aggregations, String[] fieldNames, Operator child) {
        this(groupKeys, aggregations, allGroupSelector(groupKeys), fieldNames, child);
    }

    private static int[] allGroupSelector(Expression[] groupKeys) {
        int len = groupKeys != null ? groupKeys.length : 0;
        return IntStream.range(0, len).toArray();
    }

    public static Schema inferSchema(int[] groupSelectors, Expression[] groupKeys, SqlAggFunction[] aggregations, String[] fieldNames) {
        // infer schema:
        // all aggregate function results are part of the final schema
        // keys might not be part of final schema, groupSelectors contains indexes of the keys to be put in the schema
        // the fields order is the given one for the keys (i.e., array position in groupSelector)
        //  first keys, then values (in the provided order)
        if(fieldNames.length != groupSelectors.length + aggregations.length) {
            throw new IllegalArgumentException("# of fields mismatch inferring schema for aggregation");
        }
        Field[] fields = new Field[fieldNames.length];
        int fieldIndex = 0;
        for (int groupIndex : groupSelectors) {
            fields[fieldIndex] = new Field(fieldNames[fieldIndex], groupKeys[groupIndex].type());
            fieldIndex++;
        }
        for(SqlAggFunction aggFunction : aggregations) {
            boolean isNullable = false; // TODO
            fields[fieldIndex] = new Field(fieldNames[fieldIndex], aggFunction.getType(), isNullable, false);
            fieldIndex++;
        }
        return Schema.byFields(fields);
    }

    public Expression[] getGroupKeys() {
        return groupKeys;
    }

    public int[] getGroupSelector() {
        return groupSelector;
    }

    public SqlAggFunction[] getAggregations() {
        return aggregations;
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
