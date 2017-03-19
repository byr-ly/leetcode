/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
public class Solution {
    HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
    int maxCount = 0;
    public int[] findFrequentTreeSum(TreeNode root) {
        if(root == null) return new int[0];
        List<Integer> res = new ArrayList<Integer>();
        postOrder(root,map);
        for(Integer i : map.keySet()){
            if(map.get(i) == maxCount) res.add(i);
        }
        int[] ans = new int[res.size()];
        for(int i = 0; i < res.size(); i++){
            ans[i] = res.get(i);
        }
        return ans;
    }
    
    public int postOrder(TreeNode root,HashMap<Integer,Integer> map){
        if(root == null) return 0;
        int left = postOrder(root.left,map);
        int right = postOrder(root.right,map);
        int sum = root.val + left + right;
        int count = map.getOrDefault(sum,0) + 1;
        map.put(sum,count);
        maxCount = Math.max(maxCount,count);
        return sum;
    }
}