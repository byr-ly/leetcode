public class Solution {
    public int[] intersection(int[] nums1, int[] nums2) {
        //����������HashSet����ʱ�临�Ӷȣ�Ҳ������һ��HashSet��������������Ժ�������ָ�뽵�Ϳռ临�Ӷ�
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