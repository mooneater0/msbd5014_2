package org.ip.flink.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.ip.flink.relations.IndexOfRelationAndChildRelation;
import org.ip.flink.relations.Relation;
import org.ip.flink.relations.RelationInit;
import org.ip.flink.stream.DataOperation;
import org.ip.flink.tuples.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class Algorithm extends KeyedProcessFunction<String, DataOperation, ResultTuple> {

    private ValueState<RelationInit> relationState;

    @Override
    public void open(Configuration parameters) throws Exception {
        relationState = getRuntimeContext().getState(
                new ValueStateDescriptor<RelationInit>("relationState", RelationInit.class));
    }

    @Override
    public void processElement(DataOperation dataOperation, KeyedProcessFunction<String, DataOperation, ResultTuple>.Context ctx, Collector<ResultTuple> out) throws Exception {
        RelationInit relationInit = relationState.value();
        if (relationInit == null) {
            relationInit = new RelationInit();
        }
        if (dataOperation.operation.equals("insert")) {
            if (dataOperation.data.getClass() == Customer.class) {
                insert(dataOperation.data, relationInit.customerRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Lineitem.class) {
                insert(dataOperation.data, relationInit.lineitemRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Nation.class) {
                insert(dataOperation.data, relationInit.nationRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Orders.class) {
                insert(dataOperation.data, relationInit.ordersRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Supplier.class) {
                insert(dataOperation.data, relationInit.supplierRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Region.class) {
                insert(dataOperation.data, relationInit.regionRelation, relationInit, out);
            } else {
                throw new RuntimeException("Wrong tuple class!");
            }

        } else if (dataOperation.operation.equals("delete")) {

            if (dataOperation.data.getClass() == Customer.class) {
                delete(dataOperation.data, relationInit.customerRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Lineitem.class) {
                delete(dataOperation.data, relationInit.lineitemRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Nation.class) {
                delete(dataOperation.data, relationInit.nationRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Orders.class) {
                delete(dataOperation.data, relationInit.ordersRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Supplier.class) {
                delete(dataOperation.data, relationInit.supplierRelation, relationInit, out);
            } else if (dataOperation.data.getClass() == Region.class) {
                delete(dataOperation.data, relationInit.regionRelation, relationInit, out);
            } else {
                throw new RuntimeException("Wrong tuple class!");
            }


        } else {
            throw new RuntimeException("Operation not legal!");
        }

    }


    private void insert(BaseTuple data, Relation relation, RelationInit relationInit, Collector<ResultTuple> out) throws IOException {

        Long rPK = data.getPrimaryKey();
        int childNum = relation.childRelations.size();

        boolean allAssertionKeyNotSpecialValue = true;
        if (!relation.isLeaf) {
            relation.s_counter.put(rPK, 0);

            for (int i = 0; i < childNum; i++) {
                Relation childRelation = relation.childRelations.get(i);
                IndexOfRelationAndChildRelation r_rcIndex = relation.r_rcIndexes.get(i);
                ConcurrentHashMap<Long, BaseTuple> childLiveIndex = childRelation.liveIndex;
                String rcPKName = childRelation.primaryKeyName;
                Long rcPK = data.getKey(rcPKName);
                if (r_rcIndex.index.containsKey(rcPK)) {
                    r_rcIndex.index.get(rcPK).add(rPK);
                } else {
                    CopyOnWriteArrayList<Long> newValue = new CopyOnWriteArrayList<Long>();
                    newValue.add(rPK);
                    r_rcIndex.index.put(rcPK, newValue);
                }
                if (childLiveIndex.containsKey(rcPK)) {
                    Integer sPK = relation.s_counter.get(rPK) + 1;
                    relation.s_counter.put(rPK, sPK);
                }
            }
            if (relation.s_counter.get(rPK) == childNum) {
                for (int i = 0; i < childNum; i++) {
                    Relation childRelation = relation.childRelations.get(i);
                    String rcPKName = childRelation.primaryKeyName;
                    Long rcPK = data.getKey(rcPKName);
                    BaseTuple childTuple = childRelation.liveIndex.get(rcPK);
                    ArrayList<String> assertionKeyNames = data.getAssertionKeyNames();
                    if (assertionKeyNames != null) {
                        for (String assertionKeyName : assertionKeyNames) {
                            Long assertionKeyValue = childTuple.getKey(assertionKeyName);
                            if (data.getAssertionKayValue(assertionKeyName) == null) {
                                data.setAssertionKeys(assertionKeyName, assertionKeyValue);
                            } else if (data.getAssertionKayValue(assertionKeyName) != assertionKeyValue
                                    || data.getAssertionKayValue(assertionKeyName) == BaseTuple.specialValue) {
                                data.setAssertionKeys(assertionKeyName, BaseTuple.specialValue);
                                allAssertionKeyNotSpecialValue = false;
                            }

                        }
                    }
                }
            }


        }
        if (relation.isLeaf || relation.s_counter.get(rPK) == childNum && allAssertionKeyNotSpecialValue) {
            insertUpdate(data, relation, new ResultTuple(data), relationInit, out);
        } else {
            relation.nonLiveIndex.put(rPK, data);

        }
        relationState.update(relationInit);

    }

    private void insertUpdate(BaseTuple data, Relation relation, ResultTuple joinResult, RelationInit relationInit, Collector<ResultTuple> out) throws IOException {

        Long rPK = data.getPrimaryKey();
        relation.liveIndex.put(rPK, data);
        if (relation.isRoot) {
            //add to delta Q
            out.collect(getFullResult(joinResult, "insert", relationInit));


        } else {

            for (Relation parentRelation : relation.parentRelations) {
                for (IndexOfRelationAndChildRelation rRcIndex : parentRelation.r_rcIndexes) {
                    if (rRcIndex.childRelation.equals(relation.relationName)) {
                        if (rRcIndex.index.containsKey(rPK) && rRcIndex.index.get(rPK) != null && !rRcIndex.index.get(rPK).isEmpty()) {

                            for (Long parentTuplePK : rRcIndex.index.get(rPK)) {
                                Integer sPK = parentRelation.s_counter.get(parentTuplePK) + 1;
                                parentRelation.s_counter.put(parentTuplePK, sPK);
                                int childNum = parentRelation.childRelations.size();
                                if (sPK == childNum) {
                                    boolean allAssertionKeyNotSpecialValue = true;
                                    BaseTuple parentTuple = parentRelation.nonLiveIndex.get(parentTuplePK);
                                    if (parentTuple==null)
                                        System.out.println();
                                    for (int i = 0; i < childNum; i++) {
                                        Relation parentChildRelation = parentRelation.childRelations.get(i);
                                        String rcPKName = parentChildRelation.primaryKeyName;
                                        Long rcPK = parentTuple.getKey(rcPKName);
                                        BaseTuple parentChildTuple = parentChildRelation.liveIndex.get(rcPK);
                                        ArrayList<String> assertionKeyNames = parentTuple.getAssertionKeyNames();

                                        if (assertionKeyNames != null) {
                                            for (String assertionKeyName : assertionKeyNames) {
                                                Long assertionKeyValue = parentChildTuple.getKey(assertionKeyName);
                                                if (parentTuple.getAssertionKayValue(assertionKeyName) == null) {
                                                    parentTuple.setAssertionKeys(assertionKeyName, assertionKeyValue);
                                                } else if (parentTuple.getAssertionKayValue(assertionKeyName) != assertionKeyValue
                                                        || parentTuple.getAssertionKayValue(assertionKeyName) == BaseTuple.specialValue) {
                                                    parentTuple.setAssertionKeys(assertionKeyName, BaseTuple.specialValue);
                                                    allAssertionKeyNotSpecialValue = false;
                                                }

                                            }
                                        }

                                    }
                                    if (allAssertionKeyNotSpecialValue) {
                                        parentRelation.nonLiveIndex.remove(parentTuplePK);

                                        insertUpdate(parentTuple, parentRelation, joinResult.join(parentTuple), relationInit, out);


                                    }
                                }
                            }
                        }

                    }
                }
            }
        }
        relationState.update(relationInit);
    }

    private void delete(BaseTuple data, Relation relation, RelationInit relationInit, Collector<ResultTuple> out) throws IOException {
        Long rPK = data.getPrimaryKey();
        if (relation.liveIndex.containsKey(rPK)) {
            deleteUpdate(data, relation, new ResultTuple(data), relationInit, out);
        } else {
            relation.nonLiveIndex.remove(rPK);
        }
        if (!relation.isRoot) {
            for (Relation parentRelation : relation.parentRelations) {
                for (IndexOfRelationAndChildRelation rRcIndex : parentRelation.r_rcIndexes) {
                    if (rRcIndex.childRelation.equals(relation.relationName)) {
                        rRcIndex.index.remove(rPK);
                    }

                }

            }
        }
        relationState.update(relationInit);
    }

    private void deleteUpdate(BaseTuple data, Relation relation, ResultTuple joinResult, RelationInit relationInit, Collector<ResultTuple> out) throws IOException {

        Long rPK = data.getPrimaryKey();
        relation.liveIndex.remove(rPK);
        if (relation.isRoot) {
            out.collect(getFullResult(joinResult, "delete", relationInit));
        } else {
            for (Relation parentRelation : relation.parentRelations) {
                for (IndexOfRelationAndChildRelation rRcIndex : parentRelation.r_rcIndexes) {
                    if (rRcIndex.childRelation.equals(relation.relationName)) {
                        if (rRcIndex.index.containsKey(rPK)) {
                            for (Long parentTuplePK : rRcIndex.index.get(rPK)) {
                                if (parentRelation.nonLiveIndex.containsKey(parentTuplePK)) {
                                    Integer sPK = parentRelation.s_counter.get(parentTuplePK) - 1;
                                    parentRelation.s_counter.put(parentTuplePK, sPK);
                                } else {
                                    BaseTuple parentTuple = parentRelation.liveIndex.get(parentTuplePK);
                                    Integer sPK = parentRelation.childRelations.size() - 1;
                                    parentRelation.s_counter.put(parentTuplePK, sPK);
                                    parentRelation.nonLiveIndex.put(parentTuplePK, parentTuple);
                                    deleteUpdate(parentTuple, parentRelation, joinResult.join(parentTuple), relationInit, out);
                                }
                            }
                        }
                    }
                }
            }
        }
        relationState.update(relationInit);
    }

    public static ResultTuple getFullResult(ResultTuple joinResult, String operation, RelationInit relationInit) {
        if (!joinResult.hasSupplier) {
            Supplier supplierTuple = (Supplier) relationInit.supplierRelation.liveIndex.get(joinResult.s_suppkey);
            joinResult.join(supplierTuple);
        }
        if (!joinResult.hasOrders) {
            Orders ordersTuple = (Orders) relationInit.ordersRelation.liveIndex.get(joinResult.o_orderkey);
            joinResult.join(ordersTuple);
        }
        if (!joinResult.hasCustomer) {
            Customer customerTuple = (Customer) relationInit.customerRelation.liveIndex.get(joinResult.c_custkey);
            joinResult.join(customerTuple);
        }
        if (!joinResult.hasNation) {
            Nation nationTuple = (Nation) relationInit.nationRelation.liveIndex.get(joinResult.n_nationkey);
            if (nationTuple == null)
                System.out.print(joinResult.toString() + '\n');
            joinResult.join(nationTuple);
        }
        if (!joinResult.hasRegion) {
            Region regionTuple = (Region) relationInit.regionRelation.liveIndex.get(joinResult.r_regionkey);
            joinResult.join(regionTuple);
        }
        if (operation.equals("delete")) {
            joinResult.revenue = -joinResult.revenue;
        }
        ;
        return joinResult;
    }

}
