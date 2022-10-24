package ch.usi.inf.dag.s2s.example;


import java.util.HashMap;
import java.util.Map;

public class TPCHMicroQueries {

    public static final String Q1 = """
            SELECT COUNT(*) FROM lineitem
            WHERE l_shipdate >= DATE '1995-12-01'
            """;

    public static final String Q2 = """
            SELECT sum(l_discount * l_extendedprice) FROM lineitem
            WHERE l_shipdate >= DATE '1995-12-01'
            """;

    public static final String Q3 = """
            SELECT sum(l_discount * l_extendedprice) FROM lineitem
            WHERE l_shipdate >= DATE '1995-12-01'
            AND (l_shipdate < DATE '1997-01-01')
            """;

    public static final String Q4 = """
            SELECT l_discount * l_extendedprice FROM LINEITEM
            WHERE l_shipdate >= DATE '1995-12-01'
            """;
    public static final String Q5 = """
            SELECT l_extendedprice FROM LINEITEM
            WHERE l_shipdate >= DATE '1995-12-01'
            ORDER BY l_orderkey
            LIMIT 1000
            """;
    public static final String Q6 = """
            SELECT l_discount * l_extendedprice FROM LINEITEM
            WHERE l_shipdate >= DATE '1995-12-01'
            LIMIT 1000
            """;
    public static final String Q7 = """
            SELECT SUM(o_totalprice)
            FROM LINEITEM, ORDERS
            WHERE o_orderdate >= DATE '1995-12-01'
            AND l_shipdate >= DATE '1995-12-01'
            AND o_orderkey = l_orderkey
            """;

    public static final String Q8 = """
            WITH TOP_ORDERS AS (
              SELECT * FROM ORDERS
              WHERE o_orderdate >= DATE '1995-12-01'
              ORDER BY o_totalprice DESC
              LIMIT 1000
            )

            SELECT SUM(o_totalprice) as sum_total
            FROM LINEITEM, TOP_ORDERS
            WHERE l_shipdate >= DATE '1995-12-01'
            AND o_orderkey = l_orderkey
            """;

    public static final Map<String, String> QUERIES = new HashMap<>();

    static {
        QUERIES.put("Q1", Q1);
        QUERIES.put("Q2", Q2);
        QUERIES.put("Q3", Q3);
        QUERIES.put("Q4", Q4);
        QUERIES.put("Q5", Q5);
        QUERIES.put("Q6", Q6);
        QUERIES.put("Q7", Q7);
        QUERIES.put("Q8", Q8);
    }
}
