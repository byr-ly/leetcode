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
    public int diameterOfBinaryTree(TreeNode root) {
        if(root == null) return 0;
        int cur = getLen(root.left) + getLen(root.right);
        int left = diameterOfBinaryTree(root.left);
        int right = diameterOfBinaryTree(root.right);
        return Math.max(Math.max(left,right),cur);
    }
    
    public int getLen(TreeNode root){
        if(root == null) return 0;
        else return 1 + Math.max(getLen(root.left),getLen(root.right));
    }
}