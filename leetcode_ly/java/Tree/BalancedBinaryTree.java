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
    public boolean isBalanced(TreeNode root) {
        if(root == null) return true;
        if(Math.abs(getLen(root.left) - getLen(root.right)) > 1) return false;
        else return isBalanced(root.left) && isBalanced(root.right);
    }
    
    public int getLen(TreeNode node){
        if(node == null) return 0;
        if(node.left == null && node.right == null) return 1;
        else return Math.max((1 + getLen(node.left)),(1 + getLen(node.right)));
    }
}