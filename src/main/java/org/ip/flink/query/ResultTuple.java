package org.ip.flink.query;

import org.ip.flink.tuples.*;

import java.lang.reflect.Field;

public class ResultTuple {
    public double revenue;
    //    public double c_acctbal;
    public double l_extendedprice;
    public double l_discount;
    public Long c_custkey;
    public String n_name;
    public Long o_orderkey;
    public Long s_suppkey;
    public Long n_nationkey;
    //    public Long c_nationkey;
    public Long r_regionkey;

    boolean hasLineitem = false;
    boolean hasOrders = false;
    boolean hasCustomer = false;
    boolean hasNation = false;
    boolean hasRegion = false;
    boolean hasSupplier = false;

    public ResultTuple() {
    }

    public ResultTuple(BaseTuple newTuple) {
        this.join(newTuple);
    }

    public ResultTuple join(BaseTuple newTuple) {
        if (newTuple.getClass() == Customer.class) {
            hasCustomer = true;
            n_nationkey = ((Customer) newTuple).n_nationkey;
        } else if (newTuple.getClass() == Lineitem.class) {
            hasLineitem = true;
            l_discount = ((Lineitem) newTuple).l_discount;
            l_extendedprice = ((Lineitem) newTuple).l_extendedprice;
            o_orderkey = ((Lineitem) newTuple).o_orderkey;
            s_suppkey = ((Lineitem) newTuple).s_suppkey;
            revenue = l_extendedprice * (1 - l_discount);
        } else if (newTuple.getClass() == Nation.class) {
            hasNation = true;
            n_name = ((Nation) newTuple).n_name;
            r_regionkey = ((Nation) newTuple).r_regionkey;
        } else if (newTuple.getClass() == Orders.class) {
            hasOrders = true;
            c_custkey = ((Orders) newTuple).c_custkey;
        } else if (newTuple.getClass() == Supplier.class) {
            hasSupplier = true;
            n_nationkey = ((Supplier) newTuple).n_nationkey;
        } else if (newTuple.getClass() == Region.class) {
            hasRegion = true;
        } else {
            throw new RuntimeException("Join " + newTuple.getClass() + " failed!");
        }
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Field[] fields = getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);

            try {
                String name = field.getName();
                Object value = field.get(this);

                sb.append(name).append(": ").append(value).append(", ");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 2);
        }

        return sb.toString();
    }
}
