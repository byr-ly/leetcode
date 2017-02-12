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
    public List<String> binaryTreePaths(TreeNode root) {
        List<String> res = new ArrayList<String>();
        if(root == null) return res;
        dfs(res,root,"" + root.val);
        return res;
    }
    
    public void dfs(List<String> res,TreeNode root,String s){
        if(root.left == null && root.right == null){
            res.add(s);
            return;
        }
        if(root.left != null) dfs(res,root.left,s + "->" + root.left.val);
        if(root.right != null) dfs(res,root.right,s + "->" + root.right.val);
    }
}