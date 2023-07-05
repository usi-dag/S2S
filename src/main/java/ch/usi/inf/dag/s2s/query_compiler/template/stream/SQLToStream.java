package ch.usi.inf.dag.s2s.query_compiler.template.stream;

import ch.usi.inf.dag.s2s.planner.qp.operators.*;
import ch.usi.inf.dag.s2s.query_compiler.template.Declaration;

import java.util.stream.Collectors;


public class SQLToStream {



    static public SqlStreamClassHolder generateClassMethodBody(Operator operator,
                                                               String methodName,
                                                               String methodParameters) {
        SqlOpToJavaStreamVisitor visitor = SqlOpToJavaStreamVisitor.visit(operator);
        String declarations = visitor.declarations.stream()
                .map(Declaration::toCode)
                .collect(Collectors.joining("\n"));

        String methodCode = """
                    public List<%s> %s(%s) {
                    // declarations
                    %s
                    
                    // main stream
                    %s
                    }
                    """.formatted(
                visitor.outputClassName,
                methodName,
                methodParameters,
                declarations,
                visitor.body().replace("\n", "\n\t"));

        visitor.codegenContext.addMethodFixName(methodName, methodCode);
        return new SqlStreamClassHolder(
                visitor.outputClassName, visitor.root.getSchema(), visitor.codegenContext);
    }

}