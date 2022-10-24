
mvn -q clean compile
mvn -q exec:java -Dexec.mainClass=ch.usi.inf.dag.s2s.example.ConvertTPCHMicro
mvn -q package
