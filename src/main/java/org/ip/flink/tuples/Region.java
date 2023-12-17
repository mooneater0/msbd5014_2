package org.ip.flink.tuples;

import java.util.ArrayList;
import java.util.Hashtable;

public class Region extends BaseTuple{
    public Long r_regionkey;
    public String r_name;
    public String r_comment;

    public Region(Long r_regionkey, String r_name, String r_comment) {
        this.r_regionkey = r_regionkey;
        this.r_name = r_name;
        this.r_comment = r_comment;
    }
    public Region(){

    }

    @Override
    public Long getPrimaryKey() {
        return r_regionkey;
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
