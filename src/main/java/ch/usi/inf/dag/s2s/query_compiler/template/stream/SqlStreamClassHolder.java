package ch.usi.inf.dag.s2s.query_compiler.template.stream;

import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.query_compiler.template.CodegenContext;

public class SqlStreamClassHolder {

    private final String outputClassName;
    private final Schema outputSchema;
    private final CodegenContext codegenContext;

    public SqlStreamClassHolder(String outputClassName, Schema outputSchema, CodegenContext codegenContext) {
        this.outputClassName = outputClassName;
        this.outputSchema = outputSchema;
        this.codegenContext = codegenContext;
    }

    public String asClass(String name, String packageName, String[] imports) {
        return codegenContext.asClass(name, packageName, imports, null, null);
    }

    public String getOutputClassName() {
        return outputClassName;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }
}
