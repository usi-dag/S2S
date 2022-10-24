package ch.usi.inf.dag.s2s.planner.qp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Schema {

    public static Schema reflective(Class<?> clazz) {
        ArrayList<Field> fields = new ArrayList<>();
        if(clazz.isRecord()) {
            for(java.lang.reflect.Method method : clazz.getDeclaredMethods()) {
                if(method.getParameterCount() == 0) {
                    // a getter
                    fields.add(new Field(
                            method.getName(),
                            method.getReturnType(),
                            !method.getReturnType().isPrimitive(),
                            true));
                }
            }
        } else {
            for(java.lang.reflect.Field field : clazz.getDeclaredFields()) {
                fields.add(new Field(
                        field.getName(),
                        field.getType(),
                        !field.getType().isPrimitive(),
                        false));
            }
        }
        return new Schema(fields.toArray(new Field[0]));
    }

    public static Schema byFields(Field... fields) {
        HashMap<String, Field> hm = new HashMap<>();
        Schema schema = new Schema(hm, fields);
        for (Field field : fields) {
            if(schema.hm.containsKey(field.name)) {
                throw new RuntimeException("duplicate name in schema fields: " + field.name);
            }
            schema.hm.put(field.name, field);
        }
        return schema;
    }

    final HashMap<String, Field> hm;
    final Field[] fields;

    public Schema(Field[] fields) {
        this.fields = fields;
        this.hm = new HashMap<>();
        for (Field field : fields) {
            hm.put(field.getName(), field);
        }
    }
    public Schema(Map<String, Field> hm, Field[] fields) {
        this.hm = new HashMap<>(hm);
        this.fields = fields;
    }

    public Field get(String name) {
        Field field = hm.get(name);
        if(field == null) {
            throw new RuntimeException("Unknown field: " + name + " for schema: " + this);
        }
        return field;
    }

    public Field[] getFields() {
        return fields;
    }

    public void withFieldsAsGetters() {
        hm.clear();
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].asGetter();
            hm.put(fields[i].getName(), fields[i]);
        }
    }

    @Override
    public String toString() {
        return "Schema" + hm;
    }
}
