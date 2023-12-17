package org.ip.flink.stream;

import org.ip.flink.tuples.BaseTuple;

public class DataOperation {
    public String operation;
    public BaseTuple data;


    public DataOperation(String operation, BaseTuple data) {
        this.operation = operation;
        this.data = data;
    }
}
