package ch.usi.inf.dag.s2s.query_compiler.template.stream;

import ch.usi.inf.dag.s2s.planner.qp.Field;
import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.planner.qp.expressions.Const;
import ch.usi.inf.dag.s2s.planner.qp.expressions.Expression;
import ch.usi.inf.dag.s2s.planner.qp.expressions.InputRef;
import ch.usi.inf.dag.s2s.planner.qp.operators.*;
import ch.usi.inf.dag.s2s.query_compiler.template.CodegenContext;
import ch.usi.inf.dag.s2s.query_compiler.template.Declaration;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class SQLToStream {

    private record VisitJoinInfo(
            String leftClassName,
            String leftSideCode,
            Schema leftSchema,
            String leftExprBinderPrefix,
            String rightClassName,
            String rightSideCode,
            Schema rightSchema,
            String rightExprBinderPrefix,
            String nonEquiConditionCode,
            String mapperCode,
            String mapperLeftNullCode,
            String mapperRightNullCode
    ) {}

    static public SqlStreamClassHolder generateClassMethodBody(Operator operator,
                                                               String methodName,
                                                               String methodParameters) {
        SqlOpToJavaStreamVisitor visitor = SqlOpToJavaStreamVisitor.codegen(operator);
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

    // a visitor for a single pipeline
    static class SqlOpToJavaStreamVisitor implements OperatorVisitor {
        // storage for declaration of the constants - for in-class declarations
        final Map<Const, String> constantsDeclarationMap;

        // list of in-method declarations - they must be before the code
        final LinkedList<Declaration> declarations = new LinkedList<>();

        final CodegenContext codegenContext;
        final Operator root;

        String outputClassName = "";
        String code = "";
        String finalBody;

        SqlOpToJavaStreamVisitor(CodegenContext codegenContext, Operator root) {
            this.constantsDeclarationMap = new HashMap<>();
            this.codegenContext = codegenContext;
            this.root = root;
        }
        SqlOpToJavaStreamVisitor(Operator root) {
            this(new CodegenContext(), root);
        }


        static SqlOpToJavaStreamVisitor codegen(Operator operator) {
            SqlOpToJavaStreamVisitor visitor = new SqlOpToJavaStreamVisitor(operator);
            operator.accept(visitor);
            visitor.generateBody();
            return visitor;
        }

        private void generateBody() {
            finalBody = "return " + code + ".toList();\n";
        }

        String body() {
            return finalBody;
        }


        @Override
        public void visit(ArrayTableScan scan) {
            // TODO generalize hardcoded db.
            String accessor = "db.%s".formatted(scan.source().getName());
            if(scan.source().isGetter()) {
                accessor += "()";
            }
            code = "Arrays.stream(%s)\n".formatted(accessor);
            outputClassName = scan.source().getTypeName();
        }

        @Override
        public void visit(Project project) {
            project.getChild().accept(this);

            Schema inputSchema = project.getChild().getSchema();
            Expression[] projs = project.getProjections();

            // project a tuple type: generate enclosing class
            String[] projNames = new String[projs.length];
            for (int i = 0; i < projs.length; i++) {
                projNames[i] = project.getSchema().getFields()[i].getName();
            }
            String projClassName = getOrCreateRecordType(projs, "ProjClass", projNames);
            String projClassConstructorInvocationCode = getConstructorInvocationCode(
                    projClassName, inputSchema, "row.", projs);

            // note: since it generates a record, schema fields are getters
            project.getSchema().withFieldsAsGetters();
            code += ".map(row -> %s)\n".formatted(projClassConstructorInvocationCode);
            outputClassName = projClassName;
        }

        @Override
        public void visit(Predicate predicate) {
            predicate.getChild().accept(this);
            String lambda = convertExpr(predicate.getExpression(), predicate.getChild().getSchema(), "row.");
            code += ".filter(row -> %s)\n".formatted(lambda);
        }

        @Override
        public void visit(Aggregate aggregate) {
            aggregate.getChild().accept(this);
            SqlAggFunction[] aggregations = aggregate.getAggregations();
            Expression[] groupKeyExpressions = aggregate.getGroupKeys();
            Schema inputSchema = aggregate.getChild().getSchema();
            boolean hasGroups = groupKeyExpressions.length > 0;

            // class for the collector
            // each output field in the schema should provide:
            //  - a sequence (usually of length one) of fields to be declared and initialized
            //  - a piece of code each generated method, i.e., accumulate, combine and finish

            record OutputFieldCodeGen(String declaration, String accumulate, String combine, String finish) {}

            Field[] schemaFields = aggregate.getSchema().getFields();
            List<OutputFieldCodeGen> fieldsCodeGen = new LinkedList<>();

            // first codegen for key fields
            int schemaFieldsIdx = 0;
            for(int groupIndex : aggregate.getGroupSelector()) {
                Field schemaField = schemaFields[schemaFieldsIdx++];
                String varName = schemaField.getName(), varType = schemaField.getType().getSimpleName();
                Expression groupKeyExpression = groupKeyExpressions[groupIndex];
                String exprStrAccu = convertExpr(groupKeyExpression, inputSchema, "row.");
                fieldsCodeGen.add(new OutputFieldCodeGen(
                        varType + " " + varName + ';',
                        varName + " = " + exprStrAccu + ';',
                        "",
                        ""
                ));
            }
            // then codegen aggregated fields
            for(SqlAggFunction aggFunction : aggregations) {
                SqlAggFunction.AggKind aggKind = aggFunction.getKind();
                Field schemaField = schemaFields[schemaFieldsIdx++];
                String varName = schemaField.getName(), varType = schemaField.getType().getSimpleName();
                String exprStrInit, exprStrAccu, exprStrCombine, exprStrFinish = "";
                exprStrInit = varType + " " + varName + ';';
                if(aggFunction.isCountStar()) {
                    exprStrAccu = varName + "++;";
                    exprStrCombine = varName + " += other." + varName + ';';
                } else {
                    Expression expression = aggFunction.getExpression();
                    exprStrAccu = convertExpr(expression, inputSchema, "row.");

                    if(aggKind.isAdditive()) {
                        exprStrAccu = varName + " += " + exprStrAccu + ';';
                        exprStrCombine = varName + " += other." + varName + ';';
                        if(aggKind == SqlAggFunction.AggKind.AVG) {
                            varType = "double";
                            exprStrInit = varType + " " + varName + ';';
                            // average (currently only it) requires a finisher
                            String varCtnName = varName + "_counter";
                            exprStrInit += " long " + varCtnName + " = 0; // avg counter";
                            exprStrAccu += varCtnName + "++;";
                            exprStrCombine = varName + " += other." + varName + "; " + varCtnName + " += other." + varCtnName + ';';
                            exprStrFinish = varName + " = " + varName + " / " + varCtnName + ';';
                        }
                    }
                    // MIN/MAX are the only agg functions which requires
                    //          checking if the accepted tuple is the first one
                    //          this is currently implemented as in Stream API
                    //          i.e., using an `empty` field, default=true and set to
                    //          false at the first accepted element
                    else {
                        // TODO I am assuming a non-null field - deal with nullability
                        // note: currently only min and max are not additive
                        String fun = switch (aggKind) {
                            case MIN -> "min";
                            case MAX -> "max";
                            default -> throw new IllegalStateException("Unexpected agg kind: " + aggKind);
                        };
                        String emptyVarName = codegenContext.freshName("empty");
                        fieldsCodeGen.add(new OutputFieldCodeGen("boolean %s = true;".formatted(emptyVarName),"", "", ""));
                        String exprStrAccuNotEmpty = "%s = Math.%s(%s, %s)".formatted(varName, fun, varName, exprStrAccu);
                        exprStrAccu = """
                                    if(!%s) {
                                        %s;
                                    } else {
                                        %s = false;
                                        %s = %s;
                                    }
                                    """.formatted(emptyVarName, exprStrAccuNotEmpty, emptyVarName, varName, exprStrAccu);


                        // dealing with possible empty combiner
                        String noneEmpty = "%s = Math.%s(%s, other.%s);".formatted(varName, fun, varName, varName);
                        exprStrCombine = """
                                    if(%s) { // this is empty
                                        other.%s
                                    } else if(other.%s) { // other is empty
                                        this.%s
                                    } else { // none is empty
                                        %s
                                    }
                                    """.formatted(
                                emptyVarName,
                                varName, // this is empty
                                emptyVarName,
                                varName, // other is empty
                                noneEmpty);
                    }
                }

                if(aggFunction.getFilterExpression().isPresent()) {
                    Expression filter = aggFunction.getFilterExpression().get();
                    String filterCode = convertExpr(filter, inputSchema, "row.");
                    exprStrAccu = """
                                if(%s) {
                                    %s
                                }""".formatted(filterCode, exprStrAccu);
                }
                fieldsCodeGen.add(new OutputFieldCodeGen(
                        exprStrInit,
                        exprStrAccu,
                        exprStrCombine,
                        exprStrFinish
                ));
            }

            BiFunction<Function<OutputFieldCodeGen, String>, String, String> foldJoin = (f, sep) ->
                    fieldsCodeGen
                            .stream()
                            .map(f)
                            .filter(s -> s != null && !s.isBlank())
                            .collect(Collectors.joining(sep));

            // Collector codegen
            String declarationsGenBody = foldJoin.apply(OutputFieldCodeGen::declaration, "\n\t");
            String accumulateGenBody = foldJoin.apply(OutputFieldCodeGen::accumulate, "\n\t\t");
            String combineGenBody = foldJoin.apply(OutputFieldCodeGen::combine, "\n\t\t");
            String finishGenBody = foldJoin.apply(OutputFieldCodeGen::finish, "\n\t\t");

            String collectorClassGenName = codegenContext.freshName("GroupByCollector");
            String finishGen, asCollectorGenBody;
            if(finishGenBody.isBlank()) {
                finishGen = "";
                asCollectorGenBody = MessageFormat.format("""
                            Collector.of(
                                    {0}::new,
                                    {0}::accumulate,
                                    {0}::combine)""",
                        collectorClassGenName);
            } else {
                finishGen = """
                        %s finish() {
                            %s
                            return this;
                        }
                        """.formatted(collectorClassGenName, finishGenBody);
                asCollectorGenBody = MessageFormat.format("""
                            Collector.of(
                                    {0}::new,
                                    {0}::accumulate,
                                    {0}::combine,
                                    {0}::finish)""",
                        collectorClassGenName);
            }
            String asCollectorGen = """
                    static Collector<%s, %s, %s> collector(){
                        return %s;
                    }
                    """.formatted(
                    outputClassName,
                    collectorClassGenName,
                    collectorClassGenName,
                    asCollectorGenBody);
            asCollectorGen = asCollectorGen.replace("\n", "\n\t");

            String collectorClassGenCode = String.format("""
                            public static final class %s {
                                %s
                                
                                void accumulate(%s row) {
                                    %s
                                }
                                
                                %s combine(%s other) {
                                    %s
                                    return this;
                                }
                                
                                %s
                                
                                %s
                            }
                            """,
                    collectorClassGenName,
                    declarationsGenBody,
                    outputClassName,
                    accumulateGenBody,
                    collectorClassGenName,
                    collectorClassGenName,
                    combineGenBody,
                    finishGen,
                    asCollectorGen);
            codegenContext.addInnerClass(collectorClassGenName, collectorClassGenCode);
            outputClassName = collectorClassGenName;

            String collectorInitialization = collectorClassGenName + ".collector()";

            if(!hasGroups) {
                // a non grouped aggregation
                code += ".collect(" + collectorInitialization + ")";
                code = "Stream.of(" + code + ")\n";
            } else {
                // a group-by aggregation
                // generate record class code for the key
                String keyClassName = getOrCreateRecordType(groupKeyExpressions, "AggKey");
                String groupKeyConstructorInvocationCode =  groupKeyExpressions.length == 1
                        ? convertExpr(groupKeyExpressions[0], inputSchema, "row.")
                        : getConstructorInvocationCode(keyClassName, inputSchema, "row.", groupKeyExpressions);

                code += """
                            .collect(Collectors.groupingBy(
                                row -> %s,
                                %s
                            ))
                            .values().stream()
                            """.formatted(
                        groupKeyConstructorInvocationCode,
                        collectorInitialization);
            }
        }

        @Override
        public void visit(Sort sort) {
            sort.getChild().accept(this);
            Expression[] exprs = sort.getExpressions();
            Sort.Direction[] sortDirections = sort.getDirections();

            // TODO chain of comparators without creating a field for each one
            StringBuilder composedComparator = new StringBuilder();
            String comparatorClass = "Comparator<%s>".formatted(outputClassName);
            for (int i = 0; i < exprs.length; i++) {
                String exprCode = convertExpr(exprs[i], sort.getChild().getSchema(), "row.");
                String comparatorCode = "Comparator.comparing(("+outputClassName+" row) -> " + exprCode + ")";
                if(sortDirections[i] == Sort.Direction.DESC) comparatorCode += ".reversed()";
                String name = codegenContext.addClassField("comp", comparatorClass, comparatorCode);
                if(i == 0) {
                    composedComparator.append(name);
                }  else {
                    composedComparator.append(".thenComparing(").append(name).append(")");
                }
            }
            String composedComparatorName = exprs.length < 1
                    ? codegenContext.addClassField("comp", comparatorClass, composedComparator.toString())
                    : composedComparator.toString();
            code += ".sorted(%s)\n".formatted(composedComparatorName);
        }

        @Override
        public void visit(Limit limit) {
            limit.getChild().accept(this);
            code += ".limit(%s)\n".formatted(limit.getLimit());
        }

        @Override
        public void visit(HashJoin join) {
            VisitJoinInfo visitJoinInfo = visitJoinCommonInfo(join);
            String leftExprBinderPrefixDot = visitJoinInfo.leftExprBinderPrefix + ".";
            String rightExprBinderPrefixDot = visitJoinInfo.rightExprBinderPrefix + ".";

            // join key getters
            String leftKeyGetterCode = visitJoinInfo.leftExprBinderPrefix + " -> ";
            String rightKeyGetterCode;
            if(join.leftKeyGetters().length == 1) { // assuming #leftkeys == #rightkeys
                leftKeyGetterCode += convertExpr(join.leftKeyGetters()[0], visitJoinInfo.leftSchema, leftExprBinderPrefixDot);
                rightKeyGetterCode = convertExpr(join.rightKeyGetters()[0], visitJoinInfo.rightSchema, rightExprBinderPrefixDot);
            } else {
                String keyTypeName = getOrCreateRecordType(join.leftKeyGetters(), "JoinKey");
                leftKeyGetterCode += getConstructorInvocationCode(
                        keyTypeName,
                        visitJoinInfo.leftSchema,
                        leftExprBinderPrefixDot,
                        join.leftKeyGetters());
                rightKeyGetterCode = getConstructorInvocationCode(
                        keyTypeName,
                        visitJoinInfo.rightSchema,
                        rightExprBinderPrefixDot,
                        join.rightKeyGetters());
            }

            // note currently only inner joins
            // TODO implement codegen for other joins (left, right, semi, anti)
            if(join.joinType() == Join.JoinType.INNER) {
                // first declare an empty list to create empty streams in case of missing match
                String empty = codegenContext.freshName("empty");
                String emptyType = "List<%s>".formatted(visitJoinInfo.leftClassName);
                declarations.add(new Declaration(empty, emptyType, "Collections.emptyList()"));

                // codegen collect into hashmap
                String hm = codegenContext.freshName("hashmap");
                String hmCode = ".collect(Collectors.groupingBy(%s))".formatted(leftKeyGetterCode);
                declarations.add(Declaration.withVar(hm, visitJoinInfo.leftSideCode + hmCode));
                String innerStream = visitJoinInfo.rightExprBinderPrefix + " -> %s.getOrDefault(%s, %s)\n\t".formatted(hm, rightKeyGetterCode, empty);
                innerStream += ".stream()\n\t";
                if(visitJoinInfo.nonEquiConditionCode != null) {
                    innerStream += ".filter(%s -> %s)\n\t".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.nonEquiConditionCode);
                }
                innerStream += ".map(%s -> %s)\n".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.mapperCode);
                this.code = visitJoinInfo.rightSideCode + ".flatMap(" + innerStream + ")\n";
            } else {
                throw new RuntimeException("TODO - Unsupported hash join type: " + join.joinType());
            }
        }

        @Override
        public void visit(NestedLoopJoin join) {
            VisitJoinInfo visitJoinInfo = visitJoinCommonInfo(join);
            // note currently only inner joins
            // TODO implement codegen for other joins (left, right, semi, anti)
            if(join.joinType() == Join.JoinType.INNER) {
                // collect left rows into a list
                String list = codegenContext.freshName("list");
                String emptyType = "List<%s>".formatted(visitJoinInfo.leftClassName);
                declarations.add(new Declaration(list, emptyType, visitJoinInfo.leftSideCode + ".toList()"));

                String innerStream = visitJoinInfo.rightExprBinderPrefix + " -> %s.stream()\n\t".formatted(list);
                if(visitJoinInfo.nonEquiConditionCode != null) {
                    innerStream += ".filter(%s -> %s)\n\t".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.nonEquiConditionCode);
                }
                innerStream += ".map(%s -> %s)\n".formatted(visitJoinInfo.leftExprBinderPrefix, visitJoinInfo.mapperCode);
                this.code = visitJoinInfo.rightSideCode + ".flatMap(" + innerStream + ")\n";
            } else {
                throw new RuntimeException("TODO - Unsupported nested-loop join type: " + join.joinType());
            }
        }

        private VisitJoinInfo visitJoinCommonInfo(Join join) {
            // visit the left child and get its stream code
            // TODO check if it is needed to use a second visitor instance for the left side
            //  note that using a second visitor requires using anyway the same codegenContext instance
            join.left().accept(this);
            String leftClassName = this.outputClassName;
            String leftSideCode = this.code;
            Schema leftSchema = join.left().getSchema();
            String leftExprBinderPrefix = codegenContext.freshName("l");
            // clean left visit
            this.code = this.outputClassName = "";

            // visit the right child
            join.right().accept(this);
            String rightClassName = this.outputClassName;
            String rightSideCode = this.code;
            Schema rightSchema = join.right().getSchema();
            String rightExprBinderPrefix = codegenContext.freshName("r");
            // clean right visit
            this.code = this.outputClassName = "";

            Function<InputRef, String> nameBinder = inputRef -> {
                int idx = inputRef.index();
                int nLeft = leftSchema.getFields().length;
                if(idx < nLeft) {
                    return leftExprBinderPrefix + "." + leftSchema.getFields()[idx].getName();
                } else {
                    return rightExprBinderPrefix + "." + rightSchema.getFields()[idx-nLeft].getName();
                }
            };
            Function<InputRef, String> nameBinderLeftNull = inputRef -> {
                int idx = inputRef.index();
                int nLeft = leftSchema.getFields().length;
                if(idx < nLeft) {
                    return "null";
                } else {
                    return rightExprBinderPrefix + "." + rightSchema.getFields()[idx-nLeft].getName();
                }
            };
            Function<InputRef, String> nameBinderRightNull = inputRef -> {
                int idx = inputRef.index();
                int nLeft = leftSchema.getFields().length;
                if(idx < nLeft) {
                    return leftExprBinderPrefix + "." + leftSchema.getFields()[idx].getName();
                } else {
                    return "null";
                }
            };
            // non equi condition
            String nonEquiConditionCode = join.nonEquiCondition() == null
                    ? null
                    : // "(left, right) -> " +
                    convertExpr(join.nonEquiCondition(), join.getSchema(), nameBinder);

            // gen mapped output class
            // opt candidate:
            //  generating an output class is not required if one of the following holds:
            //      - the join has no mapper, so it is marked to project only on one side
            //      - the join has a mapper which projects a single field
            //      left and right input prefixes map

            // TODO check again this logic now that there is no more "(left, right)" -> prefix
            String mapper = "";
            // mapper for left and right joins
            String mapperLeftNull = rightExprBinderPrefix + " -> ";
            String mapperRightNull = leftExprBinderPrefix + " -> ";
            if(join.isRightProjectMapper()) {
                mapper += "right";
                mapperLeftNull += "null";
                mapperRightNull += "null";
                outputClassName = rightClassName;
            } else if(join.isLeftProjectMapper()) {
                mapper += "left";
                mapperLeftNull += "null";
                mapperRightNull += "null";
                outputClassName = leftClassName;
            } else if(join.mapper().length == 1) {
                Expression expr = join.mapper()[0];
                mapper += convertExpr(expr, join.getSchema(), nameBinder);
                mapperLeftNull += convertExpr(expr, join.getSchema(), nameBinderLeftNull);
                mapperRightNull += convertExpr(expr, join.getSchema(), nameBinderRightNull);
                outputClassName = expr.type().getSimpleName();
            } else {
                // generating a class is required
                outputClassName = getOrCreateRecordType(join.mapper(), "JoinedRecord", join.getSchema());
                mapper += getConstructorInvocationCode(
                        outputClassName, join.getSchema(), nameBinder, join.mapper());

                mapperLeftNull += getConstructorInvocationCode(
                        outputClassName, join.getSchema(), nameBinderLeftNull, join.mapper());
                mapperRightNull += getConstructorInvocationCode(
                        outputClassName, join.getSchema(), nameBinderRightNull, join.mapper());
            }


            return new VisitJoinInfo(
                    leftClassName, leftSideCode, leftSchema, leftExprBinderPrefix,
                    rightClassName, rightSideCode, rightSchema, rightExprBinderPrefix,
                    nonEquiConditionCode, mapper,
                    mapperLeftNull, mapperRightNull);
        }

        private String getOrCreateRecordType(Expression[] expressions, String prefix) {
            String[] names = IntStream.range(0, expressions.length).mapToObj(i -> "_"+i).toArray(String[]::new);
            return getOrCreateRecordType(expressions, prefix, names);
        }
        private String getOrCreateRecordType(Expression[] expressions, String prefix, Schema schema) {
            String[] names = Arrays.stream(schema.getFields()).map(Field::getName).toArray(String[]::new);
            return getOrCreateRecordType(expressions, prefix, names);
        }

        private String getOrCreateRecordType(Expression[] expressions, String prefix, String[] names) {
            return createRecordType(expressions, prefix, names);
        }

        private String createRecordType(Expression[] expressions, String prefix, String[] names) {
            String recordFieldSep = expressions.length > 4 ? "\n\t\t" : "";
            String recordKeyClassFields = IntStream.range(0, expressions.length)
                    .mapToObj(i -> "%s %s".formatted(expressions[i].type().getSimpleName(), names[i]))
                    .collect(Collectors.joining("," + recordFieldSep));

            // record class code
            String className = codegenContext.freshName(prefix);
            String classCode = "public static record %s(%s%s) {}".formatted(
                    className, recordFieldSep, recordKeyClassFields);

            codegenContext.addInnerClass(className, classCode);
            return className;
        }


        private String getConstructorInvocationCode(String className,
                                                    Schema inputSchema,
                                                    String inputPrefix,
                                                    Expression[] expressions) {
            Function<Expression, String> exprConverter =
                    expression -> convertExpr(expression, inputSchema, inputPrefix);
            return getConstructorInvocationCode(className, exprConverter, expressions);
        }
        private String getConstructorInvocationCode(String className,
                                                    Schema inputSchema,
                                                    Function<InputRef, String> inputRefConverter,
                                                    Expression[] expressions) {
            Function<Expression, String> exprConverter =
                    expression -> convertExpr(expression, inputSchema, inputRefConverter);
            return getConstructorInvocationCode(className, exprConverter, expressions);
        }

        private String getConstructorInvocationCode(String className,
                                                    Function<Expression, String> exprConverter,
                                                    Expression[] expressions) {
            return  "new %s(%s)".formatted(className,
                    Arrays.stream(expressions)
                            .map(exprConverter)
                            .collect(Collectors.joining(", ")));
        }

        private String convertExpr(Expression expr, Schema inputSchema, String inputRefPrefix) {
            return SqlExprToJavaString.convert(expr, inputSchema, inputRefPrefix, constantsDeclarationMap, codegenContext);
        }

        private String convertExpr(Expression expr, Schema inputSchema, Function<InputRef, String> inputRefConverter) {
            return SqlExprToJavaString.convert(expr, inputSchema, inputRefConverter, constantsDeclarationMap, codegenContext);
        }
    }
}