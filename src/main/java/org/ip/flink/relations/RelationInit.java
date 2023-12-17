package org.ip.flink.relations;

import java.util.ArrayList;
import java.util.Hashtable;

public class RelationInit {
    public Relation lineitemRelation = new Relation("lineitem", "o_orderkey&l_linenumber");
    public Relation ordersRelation = new Relation("orders", "o_orderkey");
    public Relation customerRelation = new Relation("customer", "c_custkey");

    public Relation supplierRelation = new Relation("supplier", "s_suppkey");
    public Relation nationRelation = new Relation("nation", "n_nationkey");
    public Relation regionRelation = new Relation("region", "r_regionkey");

    public RelationInit() {
        lineitemRelation.setRoot(true);
        lineitemRelation.setLeaf(false);
        ordersRelation.setRoot(false);
        ordersRelation.setLeaf(false);
        customerRelation.setRoot(false);
        customerRelation.setLeaf(false);
        supplierRelation.setRoot(false);
        supplierRelation.setLeaf(false);
        nationRelation.setRoot(false);
        nationRelation.setLeaf(false);
        regionRelation.setRoot(false);
        regionRelation.setLeaf(true);

        lineitemRelation.childRelations.add(ordersRelation);
        lineitemRelation.childRelations.add(supplierRelation);
        ordersRelation.parentRelations.add(lineitemRelation);
        ordersRelation.childRelations.add(customerRelation);
        customerRelation.parentRelations.add(ordersRelation);
        supplierRelation.parentRelations.add(lineitemRelation);
        supplierRelation.childRelations.add(nationRelation);
        nationRelation.parentRelations.add(supplierRelation);
        nationRelation.childRelations.add(regionRelation);
        regionRelation.parentRelations.add(nationRelation);

        IndexOfRelationAndChildRelation lineitem_orders_Index = new IndexOfRelationAndChildRelation(lineitemRelation.relationName, ordersRelation.relationName);
        IndexOfRelationAndChildRelation orders_customer_Index = new IndexOfRelationAndChildRelation(ordersRelation.relationName, customerRelation.relationName);
        IndexOfRelationAndChildRelation lineitem_supplier_Index = new IndexOfRelationAndChildRelation(lineitemRelation.relationName, supplierRelation.relationName);
        IndexOfRelationAndChildRelation supplier_nation_Index = new IndexOfRelationAndChildRelation(supplierRelation.relationName, nationRelation.relationName);
        IndexOfRelationAndChildRelation nation_region__Index = new IndexOfRelationAndChildRelation(nationRelation.relationName, regionRelation.relationName);

        lineitemRelation.r_rcIndexes.add(lineitem_orders_Index);
        lineitemRelation.r_rcIndexes.add(lineitem_supplier_Index);
        ordersRelation.r_rcIndexes.add(orders_customer_Index);
        supplierRelation.r_rcIndexes.add(supplier_nation_Index);
        nationRelation.r_rcIndexes.add(nation_region__Index);

    }


}
