package ch.usi.inf.dag.s2s.planner.qp;

public class Field {

    final String name;
    final Class<?> type;
    final boolean nullable;
    final boolean isGetter;

    public Field(String name, Class<?> type) {
        this(name, type, false, false);
    }

    public Field(String name, Class<?> type, boolean nullable, boolean isGetter) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.isGetter = isGetter;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isGetter() {
        return isGetter;
    }

    public Field rename(String newName) {
        return new Field(newName, type, nullable, isGetter);
    }

    public Field asGetter() {
        return new Field(name, type, nullable, true);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type=" + type.getSimpleName() +
                ", nullable=" + nullable +
                ", isGetter=" + isGetter +
                '}';
    }
}
