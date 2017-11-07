package com.eb.bi.rs.mras.generec.hadoop;

/**
 * Created by liyang on 2017/1/9.
 */
public class Sort{
    public static int[] copy;

    //插入排序，相当于gap = 1的希尔排序
    //平均时间复杂度：O(N^2) 适用于小数组和基本有序数组的排序
    public static void insertionSort(int[] nums){
        if(nums == null || nums.length <= 1) return;
        for(int i = 1; i < nums.length; i++){
            int value = nums[i];
            int j;
            for(j = i - 1; j >= 0 && nums[j] > value; j--){
                nums[j + 1] = nums[j];
            }
            nums[j + 1] = value;
        }
    }

    //希尔排序：平均时间复杂度为O(N^2)，适用于适度大量输入数据
    public static void shellSort(int[] nums){
        if(nums == null || nums.length == 0) return;
        for(int gap = nums.length / 2; gap > 0; gap /= 2){
            for(int i = gap; i < nums.length; i++){
                int value = nums[i];
                int j;
                for(j = i - gap; j >= 0 && nums[j] > value; j -= gap){
                    nums[j + gap] = nums[j];
                }
                nums[j + gap] = value;
            }
        }
    }

    //快速排序：平均时间复杂度为O(NlogN)，数据移动少，弥补比较次数多，在C++库中被采用
    public static void quickSort(int[] nums,int left,int right){
        if(nums == null || nums.length == 0) return;
        if(left < right) {
            int low = left;
            int high = right;
            int target = nums[low];
            while(low < high){
                while (low < high && nums[high] >= target) {
                    high--;
                }
                nums[low] = nums[high];
                while (low < high && nums[low] <= target) {
                    low++;
                }
                nums[high] = nums[low];
            }
            nums[low] = target;
            quickSort(nums,left,low - 1);
            quickSort(nums,low + 1,right);
        }
    }

    //归并排序：平均时间复杂度O(NlogN)，需要附加内存
    //比较次数少（n次），元素移动次数多=>由于编译器内嵌优化，在java库中被采用
    //归并排序对于基本类型的主存排序不如快速排序，但却是外部排序的中心思想
    public static void mergeSort(int[] nums,int[] copy,int left,int right){
        if(left >= right) return;
        int mid = left + (right - left) / 2;
        mergeSort(nums,copy,left,mid);
        mergeSort(nums,copy,mid + 1,right);
        merge(nums,copy,left,mid,right);
    }

    public static void merge(int[] nums,int[] copy,int left,int pos,int right){
        int i = left;
        int j = pos + 1;
        int m = pos;
        int n = right;
        int k = left;
        while(i <= m && j <= n){
            if(nums[i] < nums[j]){
                copy[k++] = nums[i++];
            }
            else{
                copy[k++] = nums[j++];
            }
        }
        while(i <= m){
            copy[k++] = nums[i++];
        }
        while(j <= n){
            copy[k++] = nums[j++];
        }
        for(int p = left; p < k; p++){
            nums[p] = copy[p];
        }
    }

    //堆排序：平均时间复杂度：O(NlogN)
    //堆：顺序排列的完全二叉树
    public static void heapSort(int[] nums){
        for(int i = nums.length / 2; i >= 0; i--) {
            heapAdjust(nums, i, nums.length);
        }

        for(int i = nums.length - 1; i > 0; i--){
            int temp = nums[i];
            nums[i] = nums[0];
            nums[0] = temp;
            heapAdjust(nums,0,i);
        }
    }

    public static void heapAdjust(int[] nums,int parent,int length){
        int temp = nums[parent];
        int child = parent * 2 + 1;

        while(child < length){
            if(child + 1 < length && nums[child] < nums[child + 1]){
                child++;
            }
            if(temp >= nums[child]) break;
            nums[parent] = nums[child];
            parent = child;
            child = child * 2 + 1;
        }
        nums[parent] = temp;
    }

    public static void print(int[] nums){
        for(int i = 0; i < nums.length; i++){
            System.out.print(nums[i] + " ");
        }
    }

    public static void main( String[] args ){
        int[] nums = {2,4,3,8,1,9,5,7,6};
        //insertionSort(nums);
        //shellSort(nums);
        //quickSort(nums,0,nums.length - 1);
        //copy = new int[nums.length];
        //mergeSort(nums,copy,0,nums.length - 1);
        heapSort(nums);
        print(nums);
    }
}
