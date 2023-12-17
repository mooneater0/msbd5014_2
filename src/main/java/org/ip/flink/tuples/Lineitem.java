package org.ip.flink.tuples;

import java.util.ArrayList;
import java.util.Hashtable;

public class Lineitem extends BaseTuple{
    public Long o_orderkey;
    public Long l_partkey;
    public Long s_suppkey;
    // Primary Key
    public Long l_linenumber;
    public double l_quantity;
    public double l_extendedprice;
    public double l_discount;
    public double l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public String l_shipdate;
    public String l_commitdate;
    public String l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;


    public Lineitem(Long o_orderkey, Long l_partkey, Long s_suppkey, Long l_linenumber, double l_quantity, double l_extendedprice, double l_discount, double l_tax, String l_returnflag, String l_linestatus, String l_shipdate, String l_commitdate, String l_receiptdate, String l_shipinstruct, String l_shipmode, String l_comment) {
//        new Lineitem();
        this.o_orderkey = o_orderkey;
        this.l_partkey = l_partkey;
        this.s_suppkey = s_suppkey;
        this.l_linenumber = l_linenumber;
        this.l_quantity = l_quantity;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnflag = l_returnflag;
        this.l_linestatus = l_linestatus;
        this.l_shipdate = l_shipdate;
        this.l_commitdate = l_commitdate;
        this.l_receiptdate = l_receiptdate;
        this.l_shipinstruct = l_shipinstruct;
        this.l_shipmode = l_shipmode;
        this.l_comment = l_comment;
    }

    @Override
    public Long getPrimaryKey() {
        return o_orderkey * 10 + l_linenumber;
    }

    @Override
    public Long getKey(String keyName) {
        if(keyName=="l_orderkey" || keyName=="o_orderkey") return o_orderkey;
        if(keyName=="s_suppkey") return s_suppkey;
        else throw new RuntimeException("No "+keyName+" getKey function!");
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

    public Long getO_orderkey() {
        return o_orderkey;
    }

    public void setO_orderkey(Long o_orderkey) {
        this.o_orderkey = o_orderkey;
    }

    public Long getL_partkey() {
        return l_partkey;
    }

    public void setL_partkey(Long l_partkey) {
        this.l_partkey = l_partkey;
    }

    public Long getS_suppkey() {
        return s_suppkey;
    }

    public void setS_suppkey(Long s_suppkey) {
        this.s_suppkey = s_suppkey;
    }

    public Long getL_linenumber() {
        return l_linenumber;
    }

    public void setL_linenumber(Long l_linenumber) {
        this.l_linenumber = l_linenumber;
    }

    public double getL_quantity() {
        return l_quantity;
    }

    public void setL_quantity(double l_quantity) {
        this.l_quantity = l_quantity;
    }

    public double getL_extendedprice() {
        return l_extendedprice;
    }

    public void setL_extendedprice(double l_extendedprice) {
        this.l_extendedprice = l_extendedprice;
    }

    public double getL_discount() {
        return l_discount;
    }

    public void setL_discount(double l_discount) {
        this.l_discount = l_discount;
    }

    public double getL_tax() {
        return l_tax;
    }

    public void setL_tax(double l_tax) {
        this.l_tax = l_tax;
    }

    public String getL_returnflag() {
        return l_returnflag;
    }

    public void setL_returnflag(String l_returnflag) {
        this.l_returnflag = l_returnflag;
    }

    public String getL_linestatus() {
        return l_linestatus;
    }

    public void setL_linestatus(String l_linestatus) {
        this.l_linestatus = l_linestatus;
    }

    public String getL_shipdate() {
        return l_shipdate;
    }

    public void setL_shipdate(String l_shipdate) {
        this.l_shipdate = l_shipdate;
    }

    public String getL_commitdate() {
        return l_commitdate;
    }

    public void setL_commitdate(String l_commitdate) {
        this.l_commitdate = l_commitdate;
    }

    public String getL_receiptdate() {
        return l_receiptdate;
    }

    public void setL_receiptdate(String l_receiptdate) {
        this.l_receiptdate = l_receiptdate;
    }

    public String getL_shipinstruct() {
        return l_shipinstruct;
    }

    public void setL_shipinstruct(String l_shipinstruct) {
        this.l_shipinstruct = l_shipinstruct;
    }

    public String getL_shipmode() {
        return l_shipmode;
    }

    public void setL_shipmode(String l_shipmode) {
        this.l_shipmode = l_shipmode;
    }

    public String getL_comment() {
        return l_comment;
    }

    public void setL_comment(String l_comment) {
        this.l_comment = l_comment;
    }

    public Lineitem() {

        assertionKeyNames.add("n_nationkey");
//        assertionKeys.put("n_nationkey",null);
    }
}
