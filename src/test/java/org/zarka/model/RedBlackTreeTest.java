package org.zarka.model;

import org.junit.jupiter.api.BeforeEach;

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
        bst.insert(new Pair(55, "fifty-five"));
        bst.insert(new Pair(40, "forty"));
        bst.insert(new Pair(65, "sixty-five"));
        bst.insert(new Pair(60, "sixty"));
        bst.insert(new Pair(75, "seventy-five"));
        bst.insert(new Pair(57, "fifty-seven"));
        List<Integer> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i) <= res.get(i + 1));
        }
        assertEquals(bst.getSize(), 6);
    }

    @org.junit.jupiter.api.Test
    void testInsertTwo() {
        bst.insert(new Pair(33, "thirty-three"));
        bst.insert(new Pair(13, "thirteen"));
        bst.insert(new Pair(53, "fifty-three"));
        bst.insert(new Pair(11, "eleven"));
        bst.insert(new Pair(21, "twenty-one"));
        bst.insert(new Pair(41, "forty-one"));
        bst.insert(new Pair(61, "sixty-one"));
        bst.insert(new Pair(15, "fifteen"));
        bst.insert(new Pair(31, "thirty-one"));
        List<Integer> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i) <= res.get(i + 1));
        }
        assertEquals(bst.getSize(), 9);
    }

    @org.junit.jupiter.api.Test
    void deleteNodeOne() {
        bst.insert(new Pair(55, "fifty-five"));
        bst.insert(new Pair(40, "forty"));
        bst.insert(new Pair(65, "sixty-five"));
        bst.insert(new Pair(60, "sixty"));
        bst.insert(new Pair(75, "seventy-five"));
        bst.insert(new Pair(57, "fifty-seven"));
        bst.deleteNode(40);
        List<Integer> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i) <= res.get(i + 1));
        }
        assertEquals(bst.getSize(), 5);
    }

    @org.junit.jupiter.api.Test
    void deleteNodeTwo() {
        bst.insert(new Pair(55, "fifty-five"));
        bst.insert(new Pair(40, "forty"));
        bst.insert(new Pair(65, "sixty-five"));
        bst.insert(new Pair(60, "sixty"));
        bst.insert(new Pair(75, "seventy-five"));
        bst.insert(new Pair(57, "fifty-seven"));
        bst.deleteNode(40);
        bst.deleteNode(75);
        bst.deleteNode(57);
        List<Integer> res = bst.inorder();
        // make sure it's ordered
        for (int i = 0; i < res.size() - 1; i++) {
            assertTrue(res.get(i) <= res.get(i + 1));
        }
        assertEquals(bst.getSize(), 3);
    }
}