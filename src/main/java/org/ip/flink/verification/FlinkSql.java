package org.ip.flink.verification;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String startDate = "1994-01-01";
        String endDate = "1995-01-01";
        String regionName = "ASIA";


        String FilePath = "/Users/wuyue/Documents/HKUST/5014_2/msbd5014_2/input/verificationData/";
        String customerFile = FilePath + "customer.tbl";
        String lineitemFile = FilePath + "lineitem.tbl";
        String nationFile = FilePath + "nation.tbl";
        String ordersFile = FilePath + "orders.tbl";
        String supplierFile = FilePath + "supplier.tbl";
        String regionFile = FilePath + "region.tbl";

        DataStream<Tuple2<Long, Long>> customerStream = env.readTextFile(customerFile).map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String line) throws Exception {
                String[] splits = line.split("\\|");
                Tuple2<Long, Long> tuple = new Tuple2<Long, Long>(
                        Long.parseLong(splits[0]),
                        Long.parseLong(splits[3])
                );
                return tuple;
            }
        });
        DataStream<Tuple4<Long, Long, Double, Double>> lineitemStream = env.readTextFile(lineitemFile).map(new MapFunction<String, Tuple4<Long, Long, Double, Double>>() {
            @Override
            public Tuple4<Long, Long, Double, Double> map(String line) throws Exception {
                String[] splits = line.split("\\|");
                Tuple4<Long, Long, Double, Double> tuple = new Tuple4<Long, Long, Double, Double>(
                        Long.parseLong(splits[0]),
                        Long.parseLong(splits[2]),
                        Double.parseDouble(splits[5]),
                        Double.parseDouble(splits[6])
                );
                return tuple;
            }
        });
        DataStream<Tuple3<Long, String, Long>> nationStream = env.readTextFile(nationFile).map(new MapFunction<String, Tuple3<Long, String, Long>>() {
            @Override
            public Tuple3<Long, String, Long> map(String line) throws Exception {
                String[] splits = line.split("\\|");
                Tuple3<Long, String, Long> tuple = new Tuple3<Long, String, Long>(
                        Long.parseLong(splits[0]),
                        splits[1],
                        Long.parseLong(splits[2])
                );
                return tuple;
            }
        });
        DataStream<Tuple3<Long, Long, String>> ordersStream = env.readTextFile(ordersFile).map(new MapFunction<String, Tuple3<Long, Long, String>>() {
            @Override
            public Tuple3<Long, Long, String> map(String line) throws Exception {
                String[] splits = line.split("\\|");
                Tuple3<Long, Long, String> tuple = new Tuple3<Long, Long, String>(
                        Long.parseLong(splits[0]),
                        Long.parseLong(splits[1]),
                        splits[4]
                );
                return tuple;
            }
        });

        DataStream<Tuple2<Long, String>> regionStream = env.readTextFile(regionFile).map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String line) throws Exception {
                String[] splits = line.split("\\|");
                Tuple2<Long, String> tuple = new Tuple2<Long, String>(
                        Long.parseLong(splits[0]),
                        splits[1]
                );
                return tuple;
            }
        });
        DataStream<Tuple2<Long, Long>> supplierStream = env.readTextFile(supplierFile).map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String line) throws Exception {
                String[] splits = line.split("\\|");
                Tuple2<Long, Long> tuple = new Tuple2<Long, Long>(
                        Long.parseLong(splits[0]),
                        Long.parseLong(splits[3])
                );
                return tuple;
            }
        });
        tableEnv.createTemporaryView("customer", customerStream, $("c_custkey"), $("c_nationkey"));
        tableEnv.createTemporaryView("lineitem", lineitemStream, $("l_orderkey"), $("l_suppkey"), $("l_extendedprice"), $("l_discount"));
        tableEnv.createTemporaryView("nation", nationStream, $("n_nationkey"), $("n_name"), $("n_regionkey"));
        tableEnv.createTemporaryView("orders", ordersStream, $("o_orderkey"), $("o_custkey"), $("o_orderdate"));
        tableEnv.createTemporaryView("region", regionStream, $("r_regionkey"), $("r_name"));
        tableEnv.createTemporaryView("supplier", supplierStream, $("s_suppkey"), $("s_nationkey"));

        Table result = tableEnv.sqlQuery("select " +
                        "n_name, " +
                        "sum(l_extendedprice * (1 - l_discount)) as revenue " +
                        "from " +
                        "customer, " +
                        "orders, " +
                        "lineitem, " +
                        "supplier, " +
                        "nation, " +
                        "region " +
                        "where " +
                        "c_custkey = o_custkey " +
                        "and l_orderkey = o_orderkey " +
                        "and l_suppkey = s_suppkey " +
                        "and c_nationkey = s_nationkey " +
                        "and s_nationkey = n_nationkey " +
                        "and n_regionkey = r_regionkey " +
                        "and r_name = '" + regionName + "' " +
                        "and o_orderdate >= date '" + startDate + "' " +
                        "and o_orderdate < date '" + endDate + "' " +
                        "group by " + "n_name "
//                + "order by revenue desc"
        );
        result.execute().print();


    }
}
