package com.hruan.study.hbase.skipList;

import java.util.Objects;
import java.util.Random;

public final class SkipList<T> {
    private static final Random RANDOM = new Random();

    /** top level head and tail */
    private Node<T> head = new Node<>(Node.HEAD_KEY, null);
    private Node<T> tail = new Node<>(Node.TAIL_KEY, null);

    /** number of data node */
    private int numNodes = 0;
    private int numLevel = 0;

    private double probability;

    public SkipList() {
        this(0.25);
    }

    public SkipList(double probability) {
        this.probability = probability;
        horizontalLink(head, tail);
    }

    /**
     * return the node or left node for key. result must not be null.
     */
    private Node<T> findNode(int key) {
        Node<T> pointer = head;

        while (true) {
            while (pointer.right.key <= key && pointer.right.key != tail.key) {
                pointer = pointer.right;
            }
            if (pointer.down != null) {
                pointer = pointer.down;
            } else {
                break;
            }
        }
        return pointer;
    }

    /**
     * return the node for specific key or null if not exists
     */
    public Node<T> searchNode(int key) {
        Node<T> target = findNode(key);
        if (target.key == key) {
            return target;
        } else {
            return null;
        }
    }

    /**
     * return the previous node for specific key or null if not exists
     */
    public void put(int key, T value) {
        Node<T> node = findNode(key);
        if (node.key == key) {
            node.value = value;
            return;
        }

        Node<T> target = new Node<>(key, value);
        insertBehind(node, target);
        int currentLevel = 0;
        while (RANDOM.nextDouble() < probability) {
            if (currentLevel >= numLevel) {
                numLevel++;
                Node<T> newHead = new Node<>(Node.HEAD_KEY, null);
                Node<T> newTail = new Node<>(Node.TAIL_KEY, null);
                horizontalLink(newHead, newTail);
                verticalLink(newHead, head);
                verticalLink(newTail, tail);
                head = newHead;
                tail = newTail;
            }

            // find left node in top level
            while (node.up == null) {
                node = node.left;
            }
            node = node.up;

            // value is null because key is just what we need
            Node<T> next = new Node<>(key, null);
            insertBehind(node, next);
            verticalLink(next, target);

            target = next;
            currentLevel++;
        }
        numNodes++;
    }

    public int size() {
        return numNodes;
    }

    public boolean isEmpty() {
        return numNodes == 0;
    }

    public int level() {
        return numLevel;
    }

    @Override
    public String toString() {
        String str = "SkipList{" +
                "numNodes=" + numNodes +
                ", numLevel=" + numLevel +
                ", probability=" + probability +
                "}\n";

        Node<T> pointer = head;
        while (pointer != null) {
            str = str + pointer + "\n";
            pointer = pointer.down;
        }
        return str;
    }

    private void insertBehind(Node<T> left, Node<T> target) {
        target.right = left.right;
        target.left = left;
        target.right.left = target;
        target.left.right = target;
    }

    private void horizontalLink(Node<T> left, Node<T> right) {
        left.right = right;
        right.left = left;
    }

    private void verticalLink(Node<T> up, Node<T> down) {
        up.down = down;
        down.up = up;
    }

    public static final class Node<T> {
        public static final int HEAD_KEY = Integer.MIN_VALUE;
        public static final int TAIL_KEY = Integer.MAX_VALUE;

        public Node<T> left;
        public Node<T> right;
        public Node<T> up;
        public Node<T> down;

        private int key;
        private T value;

        public Node(int key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?> node = (Node<?>) o;
            return key == node.key && Objects.equals(value, node.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right, up, down, key, value);
        }

        @Override
        public String toString() {
            return "Node(" + key + "," + value + ")" + (right == null? "" : "->" + right);
        }
    }
}
