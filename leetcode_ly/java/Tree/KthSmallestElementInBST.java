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
    public int kthSmallest(TreeNode root, int k) {
        List<Integer> list = new ArrayList<Integer>();
        getList(root,list);
        return list.get(k - 1);
    }
    
    public void getList(TreeNode root,List<Integer> list){
        if(root == null) return;
        getList(root.left,list);
        list.add(root.val);
        getList(root.right,list);
        return;
    }
}