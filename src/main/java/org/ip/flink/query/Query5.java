package org.ip.flink.query;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.ip.flink.stream.DataOperation;
import org.ip.flink.stream.OperationStream;
import org.ip.flink.tuples.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Query5 {
    public static void main(String[] args) throws Exception {
        String startDate = "1994-01-01";
        String endDate = "1995-01-01 ";
        String regionName = "ASIA";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DataOperation> insertLineitem = env.addSource(new OperationStream("insert", Lineitem.class, OperationStream.insertLineitemFile));
        DataStream<DataOperation> insertOrders = env.addSource(new OperationStream("insert", Orders.class, OperationStream.insertOrdersFile)).filter(tuple -> ((Orders) tuple.data).o_orderdate.compareTo(startDate) >= 0 && ((Orders) tuple.data).o_orderdate.compareTo(endDate) < 0);
        DataStream<DataOperation> insertCustomer = env.addSource(new OperationStream("insert", Customer.class, OperationStream.insertCustomerFile));
        DataStream<DataOperation> insertSupplier = env.addSource(new OperationStream("insert", Supplier.class, OperationStream.insertSupplierFile));
        DataStream<DataOperation> insertNation = env.addSource(new OperationStream("insert", Nation.class, OperationStream.insertNationFile));
        DataStream<DataOperation> insertRegion = env.addSource(new OperationStream("insert", Region.class, OperationStream.insertRegionFile)).filter(tuple -> ((Region) tuple.data).r_name.equals(regionName));

        DataStream<DataOperation> deletaLineitem = env.addSource(new OperationStream("delete",Lineitem.class,OperationStream.deleteLineitemFile));
        DataStream<DataOperation> deleteCustomer = env.addSource(new OperationStream("delete", Customer.class, OperationStream.deleteCustomerFile));

        DataStream<DataOperation> unionedStream = insertLineitem.union(insertOrders).union(insertCustomer).union(insertSupplier).union(insertNation).union(insertRegion).union(deletaLineitem).union(deleteCustomer);

        env.setParallelism(10);
        unionedStream.keyBy(value -> "key").process(new Algorithm()).keyBy(value -> "key").process(new SumAndOrder()).print();


        env.execute();

    }
}