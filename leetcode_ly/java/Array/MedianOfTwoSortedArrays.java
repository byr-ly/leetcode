//不能用快排 因为其时间复杂度为O(nlogn)  应该使用二分查找  时间复杂度为O(logn)
public class Solution {
    public double findMedianSortedArrays(int[] nums1, int[] nums2) {
        int m = nums1.length;
        int n = nums2.length;
        return (findKNum(nums1,nums2,(m + n + 1) / 2) + findKNum(nums1,nums2,(m + n + 2) / 2)) / 2.0;
    }
    
    public int findKNum(int[] nums1,int[] nums2,int k){
        int m = nums1.length;
        int n = nums2.length;
        if(m > n) return findKNum(nums2,nums1,k);
        if(m == 0) return nums2[k - 1];
        if(k == 1) return Math.min(nums1[0],nums2[0]);
        int i = Math.min(m,k / 2);
        int j = Math.min(n,k / 2);
        if(nums1[i - 1] < nums2[j - 1]) return findKNum(Arrays.copyOfRange(nums1,i,m),nums2,k - i);
        else return findKNum(nums1,Arrays.copyOfRange(nums2,j,n),k - j);
    }
}