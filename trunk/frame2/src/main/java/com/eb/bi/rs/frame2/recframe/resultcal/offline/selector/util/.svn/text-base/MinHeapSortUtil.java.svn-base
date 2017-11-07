package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util;

import java.util.ArrayList;

public class MinHeapSortUtil {

    /**
     * 用堆排序方法  找出前N个最大的数
     *
     * @param origArr 原始输入数据
     * @param N       需要取得N个最大数
     * @return topN个最大值
     */
    public static <T extends Comparable<T>> ArrayList<T> getTopNArray(ArrayList<T> origArr, int N) {
        int len = origArr.size();
        if (N >= len) {
            return origArr;
        }
        ArrayList<T> topNArr = new ArrayList<T>();
        for (int i = 0; i < N; i++) {
            topNArr.add(i, origArr.get(i));
        }
        createHeap(topNArr);
        for (int i = N; i < len; i++) {
            T elem = origArr.get(i);
            if (elem.compareTo(topNArr.get(0)) > 0) {
                topNArr.set(0, elem);
                heapAdjust(topNArr, 0, N - 1);
            }
        }
        return topNArr;
    }

    /**
     * 创建初始堆
     *
     * @param array 原始输入数据
     */
    private static <T extends Comparable<T>> void createHeap(ArrayList<T> array) {
        int length = array.size();
        for (int i = length / 2 - 1; i >= 0; i--) {
            heapAdjust(array, i, length - 1);
        }
    }

    /**
     * 调整小顶堆，已知，heap[s...m]中除s之外都满足堆的定义，本函数使heap[s...m]成为一个小顶堆
     *
     * @param s 起始位置
     * @param m 终止位置
     */
    public static <T extends Comparable<T>> void heapAdjust(ArrayList<T> heap, int s, int m) {

        T tmp = heap.get(s);
        for (int j = 2 * s + 1; j <= m; j = 2 * j + 1) {
            if (j < m && heap.get(j + 1).compareTo(heap.get(j)) < 0) j++;
            if (tmp.compareTo(heap.get(j)) <= 0) break;
            heap.set(s, heap.get(j));
            s = j;
        }
        heap.set(s, tmp);
    }


    public static <T extends Comparable<T>> void heapSort(ArrayList<T> heap) {
        createHeap(heap);
        for (int i = heap.size() - 1; i >= 0; --i) {
            T tmp = heap.get(0);
            heap.set(0, heap.get(i));
            heap.set(i, tmp);
            heapAdjust(heap, 0, i - 1);
        }
    }

    /*public static void main(String[] args) {
    	int[] arr = {7,9,8,6,5,8,10,23,8};
    	ArrayList<Integer> arrlist = new ArrayList<Integer>();
    	for(int i = 0; i < arr.length; ++i){
    		arrlist.add(arr[i]);
    	}
    	
    	for (int i = 0; i < arrlist.size(); i++) {
			System.out.println(arrlist.get(i));
		}
    	System.out.println("====");
    	

        ArrayList<Integer> topArray = HeapSortUtil.getTopNArray(arrlist, 3);
        //打印出排序后的数组
        for(int i=0;i<topArray.size();i++){
            System.out.println(topArray.get(i));
        }
    }*/
    
}