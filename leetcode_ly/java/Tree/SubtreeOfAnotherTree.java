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
    public boolean isSubtree(TreeNode s, TreeNode t) {
        if(s == null) return false;
        if(isValid(s,t)) return true;
        else return isSubtree(s.left,t) || isSubtree(s.right,t);
    }
    
    public boolean isValid(TreeNode s, TreeNode t){
        if(s == null && t == null) return true;
        if(s == null || t == null) return false;
        if(s.val != t.val) return false;
        else{
            return isValid(s.left,t.left) && isValid(s.right,t.right);
        }
    }
}