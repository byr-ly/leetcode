public class Solution {
    public List<int[]> kSmallestPairs(int[] nums1, int[] nums2, int k) {
        List<int[]> list = new ArrayList<int[]>();
        int l1 = nums1.length;
        int l2 = nums2.length;
        if(l1 == 0 || l2 == 0) return list;
        if(k > l1 * l2) k = l1 * l2;
        
        PriorityQueue<int[]> q = new PriorityQueue<int[]>(
            new Comparator<int[]>(){
                @Override
                public int compare(int[] a,int[] b){
                    return (a[0] + a[1]) - (b[0] + b[1]);
                }
            });
        
        for(int i = 0; i < l1; i++){
            for(int j = 0; j < l2; j++){
                int[] res = new int[2];
                res[0] = nums1[i];
                res[1] = nums2[j];
                q.add(res);
            }
        }
        
        while(k != 0){
            list.add(q.poll());
            k--;
        }
        return list;
    }
}