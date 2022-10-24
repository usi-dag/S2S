package ch.usi.inf.dag.s2s.planner.qp.sources;

import ch.usi.inf.dag.s2s.planner.qp.Schema;

public class ArrayDataSource {

    private final String name;
    private final String typeName;
    private final Schema schema;
    private final boolean isGetter;

    public ArrayDataSource(String name, Schema schema, String typeName) {
        this(name, schema, typeName, false);
    }

    public ArrayDataSource(String name, Schema schema, String typeName, boolean isGetter) {
        this.name = name;
        this.schema = schema;
        this.typeName = typeName;
        this.isGetter = isGetter;
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public Schema getSchema() {
        return schema;
    }

    public boolean isGetter() {
        return isGetter;
    }
}
