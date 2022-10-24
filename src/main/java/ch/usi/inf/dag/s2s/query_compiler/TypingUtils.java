package ch.usi.inf.dag.s2s.query_compiler;

import java.util.Optional;

public class TypingUtils {

    public static Class<?> boxed(Class<?> cls) {
        return switch (cls.getSimpleName()) {
            case "int" -> Integer.class;
            case "long" -> Long.class;
            case "double" -> Double.class;
            default -> cls;
        };
    }

    public static Optional<Class<?>> maybePrimitiveOutputClass(String className) {
        return switch (className) {
            case "int" -> Optional.of(int.class);
            case "double" -> Optional.of(double.class);
            case "long" -> Optional.of(long.class);
            default -> Optional.empty();
        };
    }
}
