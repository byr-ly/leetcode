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
    public int rob(TreeNode root) {
        HashMap<TreeNode,Integer> map = new HashMap<TreeNode,Integer>();
        return robber(root,map);
    }
    
    public int robber(TreeNode root,HashMap<TreeNode,Integer> map){
        if(root == null) return 0;
        
        int val = 0;
        if(map.containsKey(root)) return map.get(root);
        if(root.left != null){
            val = val + robber(root.left.left,map) + robber(root.left.right,map);
        }
        if(root.right != null){
            val = val + robber(root.right.left,map) + robber(root.right.right,map);
        }
        
        int val1 = val + root.val;
        int val2 = robber(root.left,map) + robber(root.right,map);
        int value = Math.max(val1,val2);
        map.put(root,value);
        return value;
    }
}