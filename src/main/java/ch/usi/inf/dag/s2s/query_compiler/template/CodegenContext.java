package ch.usi.inf.dag.s2s.query_compiler.template;


import ch.usi.inf.dag.s2s.engine.StringMultiContains;

import java.sql.Date;
import java.util.*;
import java.util.stream.*;


public class CodegenContext {

    private final List<Class<?>> imports = new LinkedList<>();
    private final HashMap<String, Integer> names = new HashMap<>();
    private final HashMap<String, String> innerClasses = new HashMap<>();
    private final HashMap<String, String> methods = new HashMap<>();
    private final LinkedList<Declaration> classFields = new LinkedList<>();

    {
        List<Class<?>> importClasses = Arrays.asList(
                Arrays.class,
                List.class,
                Date.class,
                Collections.class,
                Comparator.class,
                Collector.class,
                Collectors.class,

                Stream.class,
                DoubleStream.class,
                IntStream.class,
                LongStream.class,

                StringMultiContains.class
        );
        imports.addAll(importClasses);
    }

    public String freshName(String prefix) {
        Integer occurrences = names.getOrDefault(prefix, 0);
        String name = prefix + "_" + occurrences;
        names.put(prefix, occurrences + 1);
        return name;
    }

    public void addInnerClass(String name, String code) {
        if(innerClasses.containsKey(name)) {
            throw new RuntimeException("Inner class name already used");
        }
        code = "\t" + code.replace("\n", "\n\t");
        innerClasses.put(name, code);
    }


    public void addMethodFixName(String name, String code) {
        if(methods.containsKey(name)) {
            throw new RuntimeException("Method name already used");
        }
        code = "\t" + code.replace("\n", "\n\t");
        methods.put(name, code);
    }
    public String addClassField(String name, Class<?> type, String code) {
        String fresh = freshName(name);
        imports.add(type);
        classFields.add(new Declaration(fresh, type.getSimpleName(), code));
        return fresh;
    }
    public String addClassField(String name, String typeName, String code) {
        String fresh = freshName(name);
        classFields.add(new Declaration(fresh, typeName, code));
        return fresh;
    }


    public String asClass(String name, String packageName, String[] imports, String parent, String[] interfaces) {
        if(imports == null) imports = new String[]{};
        Stream<String> thisImports = this.imports.stream().map(Class::getCanonicalName);
        String allImports = Stream.concat(thisImports, Arrays.stream(imports))
                .map("import %s;"::formatted)
                .distinct()
                .collect(Collectors.joining("\n"));
        String extendsStr = parent != null ? "extends " + parent : "";
        String implementsStr = interfaces != null ? "implements " + String.join(",", interfaces) : "";
        String classFieldsStr = classFields.stream()
                .map(declaration -> "\tstatic final " + declaration.toCode())
                .collect(Collectors.joining("\n"));
        String innerClassesStr = String.join("\n\n", innerClasses.values());
        String methodsStr = String.join("\n",
                methods.values().stream()
                        .map(s -> String.join("\n\t", s.split("\\n")))
                        .collect(Collectors.joining()));

        return """
                package %s;
                
                %s;
                
                public class %s %s %s {
                    // class fields
                %s
                    
                    // inner classes
                %s
                    
                    // methods
                %s
                }
                """
                .formatted(
                        packageName,
                        allImports,
                        name,
                        extendsStr,
                        implementsStr,
                        classFieldsStr,
                        innerClassesStr,
                        methodsStr
                );
    }



}
