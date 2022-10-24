package ch.usi.inf.dag.s2s.bench_generator;

import ch.usi.inf.dag.s2s.planner.calcite.CalcitePlanner;
import ch.usi.inf.dag.s2s.planner.calcite.ConnectionUtil;
import ch.usi.inf.dag.s2s.planner.qp.Field;
import ch.usi.inf.dag.s2s.planner.qp.PlanningException;
import ch.usi.inf.dag.s2s.planner.qp.Schema;
import ch.usi.inf.dag.s2s.planner.qp.operators.Operator;
import ch.usi.inf.dag.s2s.query_compiler.TypingUtils;
import ch.usi.inf.dag.s2s.query_compiler.template.stream.SQLToStream;
import ch.usi.inf.dag.s2s.query_compiler.template.stream.SqlStreamClassHolder;
import org.apache.calcite.sql.parser.SqlParseException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Generator {

    private final String packageName;
    private final String folderName;
    private final String testFolderName;

    public Generator(String packageName, String folderName, String testFolderName) {
        this.packageName = packageName;
        this.folderName = folderName;
        this.testFolderName = testFolderName;
    }

    public void generate(String connectionUrl, Map<String, String> queries) throws SQLException, IOException {
        String connectionGetter = "DriverManager.getConnection(\"%s\")".formatted(connectionUrl);
        Connection connection = DriverManager.getConnection(connectionUrl);
        generate(connection, connectionGetter, queries);
    }

    private void generate(Connection connection, String connectionGetter, Map<String, String> queries) throws SQLException, IOException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        String[] types = {"TABLE", "TABLE_SCHEM"};
        ResultSet tablesRS = metaData.getTables(null, null, "%", types);
        while (tablesRS.next()) {
            tables.add(tablesRS.getString("TABLE_NAME"));
        }
        generate(connection, connectionGetter, tables, queries);
    }

    private void generate(Connection connection, String connectionGetter, List<String> tableNames, Map<String, String> queries) throws IOException, SQLException {
        String packageName = this.packageName + ".queries";
        String dbPackageName = this.packageName + ".db";
        String benchPackageName = this.packageName + ".benchmarks";
        String queryFolderName = Paths.get(this.folderName, "queries").toAbsolutePath().toString();
        String testFolderName = Paths.get(this.testFolderName, "queries").toAbsolutePath().toString();
        String benchFolderName = Paths.get(this.folderName, "benchmarks").toAbsolutePath().toString();
        for(String folder : new String[]{queryFolderName, testFolderName, benchFolderName}) {
            new File(folder).mkdirs();
        }
        String[] imports = new String[] {dbPackageName + ".*", packageName + ".*"};

        // a Calcite SQL planner
        CalcitePlanner planner = new CalcitePlanner();

        // generate the schema Java files and populate the planner with DB tables
        generateSchema(connection, connectionGetter, tableNames, planner);

        // generate a test base class
        String testBaseClassCode = """
                package %s;

                import %s.*;
                import %s;

                import org.apache.calcite.adapter.java.ReflectiveSchema;
                import org.apache.calcite.jdbc.CalciteConnection;
                import org.apache.calcite.rel.RelNode;
                import org.apache.calcite.rel.RelRoot;
                import org.apache.calcite.schema.SchemaPlus;
                import org.apache.calcite.sql.SqlNode;
                import org.apache.calcite.sql.parser.SqlParser;
                import org.apache.calcite.sql.type.SqlTypeName;
                import org.apache.calcite.tools.*;
                
                import java.sql.PreparedStatement;
                import java.sql.ResultSet;
                                
                public class TestQueryBaseClass {
                    static final FrameworkConfig frameworkConfig;
                    static {
                        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
                        ReflectiveSchema schema = new ReflectiveSchema(DB.create());
                        SchemaPlus schemaPlus = rootSchema.add("root", schema);
                        
                        SqlParser.Config insensitiveParser = SqlParser.configBuilder()
                                .setCaseSensitive(false)
                        		.build();
                        frameworkConfig = Frameworks.newConfigBuilder()
                        		.parserConfig(insensitiveParser)
                        		.defaultSchema(schemaPlus)
                        		.build();
                    }
                    
                    ResultSet run(String query) throws Exception {
                        Planner planner = Frameworks.getPlanner(frameworkConfig);
                        SqlNode sqlNode = planner.parse(query);
                        SqlNode sqlNodeValidated = planner.validate(sqlNode);
                        RelRoot relRoot = planner.rel(sqlNodeValidated);
                        RelNode relNode = relRoot.project();
                        PreparedStatement run = RelRunners.run(relNode);
                        return run.executeQuery();
                    }
                }
                """.formatted(packageName, dbPackageName, ConnectionUtil.class.getCanonicalName());
        genClass(testFolderName, "TestQueryBaseClass", testBaseClassCode);

        // generate the implementations
        for (Map.Entry<String, String> entry : queries.entrySet()) {
            String qName = entry.getKey();
            String query = entry.getValue();

            try {
                Operator operator = planner.plan(query);
                SqlStreamClassHolder generatedSqlStreamClass = SQLToStream.generateClassMethodBody(
                        operator, "exec", "DB db");

                String className = "Query_" + qName;
                String genCode = generatedSqlStreamClass.asClass(className, packageName, imports);
                genClass(queryFolderName, className, genCode);

                String outputRowClassName = className + "." + generatedSqlStreamClass.getOutputClassName();

                // generate tests
                String testClassName = "Test" + className;
                String rowAssertions = Arrays.stream(generatedSqlStreamClass.getOutputSchema().getFields())
                        .map(field -> {
                            String streamField = "streamRow." + field.getName() + (field.isGetter() ? "()" : "");
                            String rsField = "rs." + getResultSetMethodForField(field) + "(\"" + field.getName() + "\")";
                            if(field.getType() == Date.class) {
                                streamField += ".toString()";
                                rsField += ".toString()";
                            }
                            return "assertEquals(%s, %s);".formatted(streamField, rsField);
                        })
                        .collect(Collectors.joining("\n\t"));
                String streamGetter = """
                    %s executor = new %s();
                    List<%s> rsStream = executor.exec(DB.create());
                    """.formatted(className, className, outputRowClassName);

                String testClassMethodCode = """
                    // Execute the query on the given connection
                    ResultSet rs = run(\"\"\"
                        %s
                    \"\"\");
                    // Execute the query using the Stream API with the generated class
                    %s
                    for(%s streamRow : rsStream) {
                        assertTrue(rs.next(), "Result sets have different size (stream one is larger)");
                        %s
                    }
                    assertFalse(rs.next(), "Result sets have different size (stream one is smaller)");
                    """.formatted(
                        query,
                        streamGetter,
                        outputRowClassName,
                        rowAssertions);

                testClassMethodCode = testClassMethodCode.replace("\n", "\n\t\t");

                String testClassCode =
                    """
                    package %s;
                    
                    import %s.*;
                    
                    import static org.junit.jupiter.api.Assertions.*;
                    import org.junit.jupiter.api.*;
                    import java.sql.*;
                    import java.util.*;
                                                                                       
                    public class %s extends %s.TestQueryBaseClass {
                        
                        @Test
                        @Timeout(60) // 1 minute
                        public void testQuery() throws Exception {
                            %s
                        }
                    }
                    """.formatted(packageName, dbPackageName, testClassName, packageName,  testClassMethodCode);
                genClass(testFolderName, testClassName, testClassCode);

                // generate benchmarks
                String benchClassName = "Bench" + className;
                String importStr = Arrays.stream(imports)
                        .map("import %s;"::formatted)
                        .collect(Collectors.joining("\n"));
                String benchClassCode = """
                    package %s;
                                                    
                    %s
                    import org.openjdk.jmh.annotations.*;
                         
                    import java.util.List;

                    @State(Scope.Thread)
                    public class %s {
                        DB db = DB.create();
                        %s executor = new %s();
                             
                        @Benchmark
                        public List run() {
                            return executor.exec(db);
                        }
                    }
                    """.formatted(benchPackageName, importStr, benchClassName, className, className);

                genClass(benchFolderName, benchClassName, benchClassCode);
            } catch (SqlParseException e) {
                System.out.println("Error parsing query:\n" + qName);
                System.out.println(e.getMessage());
            } catch (PlanningException e) {
                System.out.println("Error converting a Calcite plan into Stream plan:\n" + qName);
                System.out.println(e.getMessage());
            } catch (Exception e) {
                System.out.println("Exception:\n" + qName);
                System.out.println(e.getMessage());
            }
        }
    }


    private void generateSchema(Connection connection, String connectionGetter, List<String> tableNames, CalcitePlanner planner) throws SQLException, IOException {
        String packageName = this.packageName + ".db";
        String folderName = Paths.get(this.folderName, "db").toAbsolutePath().toString();

        // create the codegen folder if it does not exist
        new File(folderName).mkdirs();

        // get schemas from the database and add tables to Calcite planner
        Map<String, Schema> schemaMap = new HashMap<>();
        for (String t : tableNames) {
            Schema schema = asSchema(connection, t);
            schemaMap.put(t, schema);
            planner.addTable(t, schema);
        }

        // generate class files for the tables
        for(Map.Entry<String, Schema> entry : schemaMap.entrySet()) {
            String clsName = entry.getKey();
            Schema schema = entry.getValue();

            // record class code
            String[] resultSetToParams = new String[schema.getFields().length];
            for (int i = 0; i < resultSetToParams.length; i++) {
                Field f = schema.getFields()[i];
                String method = getResultSetMethodForField(f);
                resultSetToParams[i] = "rs.%s(%s)".formatted(method, i+1);
                if(f.getType() == Date.class) {
                    resultSetToParams[i] = "new %s((int) LocalDate.parse(rs.getObject(%s).toString()).toEpochDay())".formatted(
                            Date.class.getCanonicalName(), i+1);
                }
            }
            String createRecordArray = """
                    // create from database connection
                    static %s[] create(Connection con) throws SQLException {
                        ResultSet rs;
                        Statement stm = con.createStatement();
                        rs = stm.executeQuery("select count(*) from %s");
                        rs.next();
                        int ctn = rs.getInt(1);
                        %s[] data = new %s[ctn];
                        rs = stm.executeQuery("select * from %s");
                        int i=0;
                        while(rs.next()) {
                            data[i++] = new %s(%s);
                        }
                        return data;
                    }
                    """.formatted(
                    clsName, clsName, clsName, clsName, clsName, clsName,
                    String.join(",\n\t\t\t\t", resultSetToParams));

            String classFieldsDeclaration = Stream.of(schema.getFields())
                    .map(f -> "public final %s %s;".formatted(f.getType().getCanonicalName(), f.getName()))
                    .collect(Collectors.joining("\n\t"));
            String classFieldsParams = Stream.of(schema.getFields())
                    .map(f -> "%s %s".formatted(f.getType().getCanonicalName(), f.getName()))
                    .collect(Collectors.joining(", "));
            String classFieldsInitialization = Stream.of(schema.getFields())
                    .map(f -> "this.%s = %s;".formatted(f.getName(), f.getName()))
                    .collect(Collectors.joining("\n\t\t"));

            String recordDef = """
                    package %s;
                    
                    import java.sql.*;
                    import java.time.LocalDate;
                    
                    public final class %s {
                        // fields
                    %s
                    
                        public %s(%s) {
                        %s
                        }
                    
                        %s
                    }
                    """.formatted(
                    packageName,
                    clsName,
                    classFieldsDeclaration,
                    clsName,
                    classFieldsParams,
                    classFieldsInitialization,
                    createRecordArray.replace("\n", "\n\t"));

            genClass(folderName, clsName, recordDef);
        }

        // generate a class file for the in-memory db, i.e., an array for each generated table
        String tableDeclarations = tableNames.stream()
                .map(t -> "public final %s[] %s;".formatted(t, t))
                .collect(Collectors.joining("\n\t"));
        String tableParams = tableNames.stream()
                .map(t -> "%s[] %s".formatted(t, t))
                .collect(Collectors.joining(", "));
        String tableInitializations = tableNames.stream()
                .map(t -> "this.%s = %s;".formatted(t, t))
                .collect(Collectors.joining("\n\t\t"));
        String tableCreations = tableNames.stream()
                .map(t -> "%s.%s.create(con)".formatted(packageName, t))
                .collect(Collectors.joining(",\n\t\t\t\t\t"));
        String dbClassCode = """
                package %s;
                import java.sql.*;
                
                public final class DB {
                        
                    private static DB INSTANCE;
                    
                    // tables
                    %s
                    
                    public DB(%s) {
                        %s
                    }
                    
                    public static DB create() {
                        try {
                            if(INSTANCE == null) {
                                Connection con = connection();
                                return new DB(
                                    %s);
                            }
                            return INSTANCE;
                        } catch(SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    
                    public static Connection connection() {
                        try {
                            return %s;
                        } catch(SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                """.formatted(
                packageName,
                tableDeclarations,
                tableParams,
                tableInitializations,
                tableCreations,
                connectionGetter);
        genClass(folderName, "DB", dbClassCode);
    }

    private void genClass(String folder, String name, String code) throws IOException {
        name = Path.of(folder, name).toFile().getAbsolutePath();
        FileWriter writer = new FileWriter(name + ".java");
        writer.write(code);
        writer.close();
    }
    private Schema asSchema(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName, "%");
        List<Field> fieldList = new ArrayList<>();
        while (rs.next()) {
            String fieldName = rs.getString("COLUMN_NAME");
            Class<?> fieldClass = switch (rs.getString("TYPE_NAME")) {
                case "INTEGER" -> int.class;
                case "DECIMAL", "DOUBLE PRECISION" -> double.class;
                case "BIGINT" -> long.class;
                case "DATE" -> Date.class;
                case "TEXT", "CHAR", "VARCHAR" -> String.class;
                default -> throw new IllegalStateException("Unexpected SQL field type: " +
                        rs.getString("TYPE_NAME"));
            };
            boolean nullable = !"NO".equals(rs.getString("IS_NULLABLE"));
            if(nullable)
                fieldClass = TypingUtils.boxed(fieldClass);
            boolean isGetter = false;
            fieldList.add(new Field(fieldName, fieldClass, nullable, isGetter));
        }
        return Schema.byFields(fieldList.toArray(Field[]::new));
    }

    private String getResultSetMethodForField(Field field) {
        return switch (field.getType().getSimpleName()) {
            case "int",     "Integer"   -> "getInt";
            case "double",  "Double"    -> "getDouble";
            case "long",    "Long"      -> "getLong";
            case "Date"                 -> "getDate";
            case "String"               -> "getString";

            default -> throw new IllegalStateException("Unexpected type: " + field.getType());
        };
    }
}
