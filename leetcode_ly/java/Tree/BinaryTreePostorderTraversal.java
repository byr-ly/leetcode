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
    public List<Integer> postorderTraversal(TreeNode root) {
        List<Integer> list =  new ArrayList<Integer>();
        if(root == null) return list;
        postorder(root,list);
        return list;
    }
    
    public void postorder(TreeNode root,List<Integer> list){
        if(root == null) return;
        postorder(root.left,list);
        postorder(root.right,list);
        list.add(root.val);
        return;
    }
}