package ch.usi.inf.dag.s2s.example;


import ch.usi.inf.dag.s2s.bench_generator.Generator;

import java.io.File;

public class ConvertTPCHMicro {

    public static final String URL = "jdbc:sqlite:TPCH-sqlite/TPC-H.db";

    public static void main(String[] args) throws Exception {
        String packageFolder = "generated_queries_example";

        File baseOutputDir = new File(System.getProperty("basedir"), "target/generated-sources/annotations");
        File outputDir = new File(baseOutputDir, packageFolder);
        outputDir.mkdirs();
        File baseTestOutputDir = new File(System.getProperty("basedir"), "target/generated-test-sources/test-annotations");
        File testOutputDir = new File(baseTestOutputDir, packageFolder);
        testOutputDir.mkdirs();

        Generator generator = new Generator(packageFolder, outputDir.getAbsolutePath(), testOutputDir.getAbsolutePath());
        generator.generate(URL, TPCHMicroQueries.QUERIES);
    }
}