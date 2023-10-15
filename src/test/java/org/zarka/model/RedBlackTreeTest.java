package org.zarka.model;

import org.junit.jupiter.api.BeforeEach;
import org.zarka.avro.Data;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedBlackTreeTest {
    RedBlackTree bst;

    @BeforeEach
    void setUp() {
        bst = new RedBlackTree();
    }

    @org.junit.jupiter.api.Test
    void testInsertOne() {
        Data data = Data.newBuilder()
                .setKey("1")
                .setValue("low")
                .build();
        Data data2 = new Data("2", "low");
        Data data3 = new Data("3", "low");
        Data data4 = new Data("4", "low");
        Data data5 = new Data("5", "low");
        Data data6 = new Data("6", "low");
        bst.insert(data);
        bst.insert(data2);
        bst.insert(data3);
        bst.insert(data4);
        bst.insert(data5);
        bst.insert(data6);
        List<Data> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i).getKey().toString().compareTo(res.get(i + 1).getKey().toString()) <= 0);
        }
        assertEquals(bst.getNodesCount(), 6);
    }

    @org.junit.jupiter.api.Test
    void deleteNodeOne() {
        Data data2 = new Data("2", "low");
        Data data3 = new Data("3", "low");
        Data data4 = new Data("4", "low");
        Data data5 = new Data("5", "low");
        Data data6 = new Data("6", "low");
        bst.insert(data2);
        bst.insert(data3);
        bst.insert(data4);
        bst.insert(data5);
        bst.insert(data6);
        bst.deleteNode("4");
        List<Data> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i).getKey().toString().compareTo(res.get(i + 1).getKey().toString()) <= 0);
        }
        assertEquals(bst.getNodesCount(), 4);
    }

    @org.junit.jupiter.api.Test
    void deleteNodeTwo() {
        Data data2 = new Data("2", "low");
        Data data3 = new Data("3", "low");
        Data data4 = new Data("4", "low");
        Data data5 = new Data("5", "low");
        Data data6 = new Data("6", "low");
        bst.insert(data2);
        bst.insert(data3);
        bst.insert(data4);
        bst.insert(data5);
        bst.insert(data6);
        bst.deleteNode("3");
        bst.deleteNode("4");
        bst.deleteNode("5");
        List<Data> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i).getKey().toString().compareTo(res.get(i + 1).getKey().toString()) <= 0);
        }
        assertEquals(bst.getNodesCount(), 2);
    }
}