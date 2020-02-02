package com.hruan.study.hbase.skipList;

public class SkipListTest {
    public static void main(String[] args) {
        double[] cases = new double[] { 0.01, 0.05, 0.1, 0.125, 0.25, 0.5, 0.75, 0.95, 0.99 };

        for (double c : cases) {
            SkipList<Integer> skipList = new SkipList<>(c);
            testPut(skipList);
        }
    }

    private static void testPut(SkipList<Integer> skipList) {
        for (int i = 0; i < 100; i ++) {
            skipList.put(i, i);
        }
        System.out.println(skipList);
        System.out.println("\n\n\n");
    }
}
