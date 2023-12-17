package org.ip.flink.tuples;

import java.lang.reflect.Field;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Hashtable;

public abstract class BaseTuple {
    public static Long specialValue=new Long(-1);
    public BaseTuple() {

    }
    public abstract Long getPrimaryKey();

    public abstract Long getKey(String keyName);
    public abstract ArrayList<String> getAssertionKeyNames();
    public abstract Long getAssertionKayValue(String assertionKeyName);
    public abstract void setAssertionKeys(String assertionKeyName,Long assertionKeyValue);
    public String toString(){
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
