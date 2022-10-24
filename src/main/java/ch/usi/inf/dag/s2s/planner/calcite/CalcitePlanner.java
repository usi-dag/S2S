package ch.usi.inf.dag.s2s.planner.calcite;

import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.planner.qp.operators.Operator;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParseException;

public class CalcitePlanner {
    private final CalciteConnection connection = ConnectionUtil.connection();
    private final SchemaPlus rootSchema = connection.getRootSchema();

    public void addTable(String name, Schema schema) {
        rootSchema.add(name, asTable(schema));
    }

    private static Table asTable(Schema schema) {
        return new SchemaTable(schema);
    }

    public Operator plan(String query) throws SqlParseException {
        SqlToCalciteRel sqlToCalciteRel = new SqlToCalciteRel(rootSchema);
        RelNode relNode = sqlToCalciteRel.convert(query);
        return CalciteRelToInternalQP.convert(relNode);
    }
}
