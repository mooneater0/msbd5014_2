package org.ip.flink.relations;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class IndexOfRelationAndChildRelation {
    public String baseRelation;
    public String childRelation;
    public Hashtable<Long, CopyOnWriteArrayList<Long>> index;

    public IndexOfRelationAndChildRelation(String baseRelationName, String childRelationName) {
        this.baseRelation = baseRelationName;
        this.childRelation = childRelationName;
        this.index = new Hashtable<Long, CopyOnWriteArrayList<Long>>();
    }

}
