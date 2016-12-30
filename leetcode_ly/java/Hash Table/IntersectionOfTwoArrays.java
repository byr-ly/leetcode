public class Solution {
    public int[] intersection(int[] nums1, int[] nums2) {
        //可以用两个HashSet降低时间复杂度，也可以用一个HashSet对数组进行排序以后用两个指针降低空间复杂度
        HashSet<Integer> set = new HashSet<Integer>();
        HashSet<Integer> insec = new HashSet<Integer>();
        
        for(int i = 0; i < nums1.length; i++){
            set.add(nums1[i]);
        }
        
        for(int j = 0; j < nums2.length; j++){
            if(set.contains(nums2[j])) insec.add(nums2[j]);
        }
        
        int[] res = new int[insec.size()];
        int i = 0;
        for(Integer k : insec){
            res[i++] = k;
        }
        return res;
    }
}