public class Solution {
    public int[] intersect(int[] nums1, int[] nums2) {
        ArrayList<Integer> insec = new ArrayList<Integer>();
        Arrays.sort(nums1);
        Arrays.sort(nums2);
        int i = 0;
        int j = 0;
        while(i < nums1.length && j < nums2.length){
            if(nums1[i] < nums2[j]) i++;
            else if(nums1[i] > nums2[j]) j++;
            else if(nums1[i] == nums2[j]){
                insec.add(nums1[i]);
                i++;
                j++;
            }
        }
        
        int[] res = new int[insec.size()];
        int p = 0;
        for(int k : insec){
            res[p++] = k;
        }
        return res;
    }
}