package org.ip.flink.tuples;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Objects;

public class Customer extends BaseTuple {
    public Long c_custkey;
    public String c_name;
    public String c_address;
    public Long n_nationkey;
    public String c_phone;
    public double c_acctbal;
    public String c_mktsegment;
    public String c_comment;

    public Customer(Long c_custkey, String c_name, String c_address, Long n_nationkey, String c_phone, double c_acctbal, String c_mktsegment, String c_comment) {
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.n_nationkey = n_nationkey;
        this.c_phone = c_phone;
        this.c_acctbal = c_acctbal;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
    }

    @Override
    public Long getPrimaryKey() {
        return c_custkey;
    }

    @Override
    public Long getKey(String keyName) {
        if (Objects.equals(keyName, "c_custkey")) return c_custkey;
        else if (Objects.equals(keyName, "c_nationkey") || Objects.equals(keyName, "n_nationkey")) return n_nationkey;
        else throw new RuntimeException("No " + keyName + " getKey function!");
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

    public Long getC_custkey() {
        return c_custkey;
    }

    public void setC_custkey(Long c_custkey) {
        this.c_custkey = c_custkey;
    }

    public String getC_name() {
        return c_name;
    }

    public void setC_name(String c_name) {
        this.c_name = c_name;
    }

    public String getC_address() {
        return c_address;
    }

    public void setC_address(String c_address) {
        this.c_address = c_address;
    }

    public Long getN_nationkey() {
        return n_nationkey;
    }

    public void setN_nationkey(Long n_nationkey) {
        this.n_nationkey = n_nationkey;
    }

    public String getC_phone() {
        return c_phone;
    }

    public void setC_phone(String c_phone) {
        this.c_phone = c_phone;
    }

    public double getC_acctbal() {
        return c_acctbal;
    }

    public void setC_acctbal(double c_acctbal) {
        this.c_acctbal = c_acctbal;
    }

    public String getC_mktsegment() {
        return c_mktsegment;
    }

    public void setC_mktsegment(String c_mktsegment) {
        this.c_mktsegment = c_mktsegment;
    }

    public String getC_comment() {
        return c_comment;
    }

    public void setC_comment(String c_comment) {
        this.c_comment = c_comment;
    }

    public Customer() {
    }
}
