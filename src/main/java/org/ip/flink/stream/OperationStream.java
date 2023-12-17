package org.ip.flink.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.ip.flink.tuples.BaseTuple;
import org.ip.flink.tuples.Customer;
import org.ip.flink.tuples.Lineitem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Field;

public class OperationStream implements SourceFunction<DataOperation> {
    public static String insertFilePath = "/Users/wuyue/Documents/HKUST/5014_2/msbd5014_2/input/processedData/insert/";
    public static String deleteFilePath = "/Users/wuyue/Documents/HKUST/5014_2/msbd5014_2/input/processedData/delete/";
    public static String insertCustomerFile = insertFilePath + "customer.tbl";
    public static String deleteCustomerFile = deleteFilePath + "customer.tbl";
    public static String insertLineitemFile = insertFilePath + "lineitem.tbl";
    public static String deleteLineitemFile = deleteFilePath + "lineitem.tbl";
    public static String insertNationFile = insertFilePath + "nation.tbl";
    public static String insertOrdersFile = insertFilePath + "orders.tbl";
    public static String insertSupplierFile = insertFilePath + "supplier.tbl";
    public static String insertRegionFile = insertFilePath + "region.tbl";

    String operation;
    Class<?> tClass;
    String filename;

    public OperationStream(String operation, Class<?> tClass, String filename) {
        this.operation = operation;
        this.tClass = tClass;
        this.filename = filename;
    }

    @Override
    public void run(SourceContext<DataOperation> ctx) throws Exception {
        if (operation.equals("delete")) {
            // To make sure deleting tuples are processed after inserting tuples
            if (tClass == Customer.class) Thread.sleep(5000L);
            if (tClass == Lineitem.class) Thread.sleep(10000L);
        }
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split(">");
            String operation = split[0];
            String[] values = split[1].split("\\|");

            Object newTuple = tClass.getDeclaredConstructor().newInstance();
            Field[] fields = tClass.getDeclaredFields();
            for (int i = 0; i < values.length; i++) {
                fields[i].setAccessible(true);
                if (fields[i].getType() == String.class) {
                    fields[i].set(newTuple, values[i]);
                } else if (fields[i].getType() == Long.class) {
                    fields[i].set(newTuple, Long.parseLong(values[i]));
                } else if (fields[i].getType() == double.class) {
                    fields[i].set(newTuple, Double.parseDouble(values[i]));
                } else {
                    throw new RuntimeException("Invalid type: " + fields[i].getType());
                }
            }

            DataOperation dataOperation = new DataOperation(operation, (BaseTuple) newTuple);
            ctx.collect(dataOperation);
        }


    }

    @Override
    public void cancel() {

    }
}
