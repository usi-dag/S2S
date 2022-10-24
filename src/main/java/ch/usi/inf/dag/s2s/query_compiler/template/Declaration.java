package ch.usi.inf.dag.s2s.query_compiler.template;

public record Declaration(String name, String typeName, String initCode) {
    // manage a field declaration and initialization

    public static Declaration withVar(String name, String initCode) {
        return new Declaration(name, "var", initCode);
    }

    public String toCode() {
        return "%s %s = %s;".formatted(typeName, name, initCode).replaceAll("\n\t*;", ";\n");
    }
}