package org.ip.flink.tuples;

import java.util.ArrayList;
import java.util.Hashtable;

public class Nation extends BaseTuple{
    public Long n_nationkey;
    public String n_name;
    public Long r_regionkey;
    public String n_comment;

    public Nation(Long n_nationkey, String n_name, Long r_regionkey, String n_comment) {
        this.n_nationkey = n_nationkey;
        this.n_name = n_name;
        this.r_regionkey = r_regionkey;
        this.n_comment = n_comment;
        assertionKeys=new Hashtable<>();
        assertionKeyNames=new ArrayList<>();
    }

    @Override
    public Long getPrimaryKey() {
        return n_nationkey;
    }

    @Override
    public Long getKey(String keyName) {
        if(keyName=="r_regionkey") return r_regionkey;
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



}
