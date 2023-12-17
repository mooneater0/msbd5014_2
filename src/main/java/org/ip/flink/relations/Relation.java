package org.ip.flink.relations;

import org.ip.flink.tuples.BaseTuple;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

public class Relation {
    public String relationName;
    public String primaryKeyName;
    public ArrayList<Relation> parentRelations = new ArrayList<Relation>();
    public ArrayList<Relation> childRelations = new ArrayList<Relation>();
//    public  Hashtable<Long, BaseTuple> fullIndex = new Hashtable<>();
    public ConcurrentHashMap<Long, BaseTuple> liveIndex = new ConcurrentHashMap<Long, BaseTuple>();
    public ConcurrentHashMap<Long, BaseTuple> nonLiveIndex = new ConcurrentHashMap<Long, BaseTuple>();


    public ConcurrentHashMap<Long, Integer> s_counter = new ConcurrentHashMap<Long, Integer>();

    public ArrayList<IndexOfRelationAndChildRelation> r_rcIndexes = new ArrayList<IndexOfRelationAndChildRelation>();


    public boolean isRoot=false;
    public boolean isLeaf=false;


    public void setR_rcIndexes(ArrayList<IndexOfRelationAndChildRelation> r_rcIndexes) {
        this.r_rcIndexes = r_rcIndexes;
    }




    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public Relation(String relationName, String primaryKeyName) {
        this.relationName = relationName;
        this.primaryKeyName = primaryKeyName;
    }
}
