package org.ip.flink.tuples;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Objects;

public class Orders extends BaseTuple{
    public Long o_orderkey;
    // Foreign Key to C_CUSTKEY
    public Long c_custkey;
    public String o_orderstatus;
    public double o_totalprice;
    public String o_orderdate;
    public String o_orderpriority;
    public String o_clerk;
    public Long o_shippriority;
    public String o_comment;

    public Orders(Long o_orderkey, Long c_custkey, String o_orderstatus, double o_totalprice, String o_orderdate, String o_orderpriority, String o_clerk, Long o_shippriority, String o_comment) {
        this.o_orderkey = o_orderkey;
        this.c_custkey = c_custkey;
        this.o_orderstatus = o_orderstatus;
        this.o_totalprice = o_totalprice;
        this.o_orderdate = o_orderdate;
        this.o_orderpriority = o_orderpriority;
        this.o_clerk = o_clerk;
        this.o_shippriority = o_shippriority;
        this.o_comment = o_comment;

    }

    @Override
    public Long getPrimaryKey() {
        return o_orderkey;
    }

    @Override
    public Long getKey(String keyName) {
        if(Objects.equals(keyName, "c_custkey") || Objects.equals(keyName, "c_custkey")) return c_custkey;
        if(Objects.equals(keyName,"n_nationkey")) return getAssertionKayValue(keyName);
        else {
            System.out.print(keyName);
            throw new RuntimeException("No " + keyName + " getKey function!");
        }
    }

    public ArrayList<String> assertionKeyNames=new ArrayList<>();
    public Hashtable<String, Long> assertionKeys=new Hashtable<>();

    @Override
    public ArrayList<String> getAssertionKeyNames() {
        return assertionKeyNames;
    }
    @Override
    public Long getAssertionKayValue(String assertionKeyName) {
        return assertionKeys.get(assertionKeyName);
    }
    @Override
    public void setAssertionKeys(String assertionKeyName, Long assertionKeyValue) {
        assertionKeys.put(assertionKeyName,assertionKeyValue);
    }

}
