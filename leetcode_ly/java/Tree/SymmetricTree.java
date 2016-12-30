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
    public boolean isSymmetric(TreeNode root) {
        if(root == null) return true;
        TreeNode node = reverse(root.left);
        root.left = node;
        return isSame(root.left,root.right);
    }
    
    //将左子树翻转即左右子树完全一样
    public TreeNode reverse(TreeNode root){
        if(root == null) return null;
        TreeNode temp = root.left;
        root.left = root.right;
        root.right = temp;
        root.left = reverse(root.left);
        root.right = reverse(root.right);
        return root;
    }
    
    public boolean isSame(TreeNode left,TreeNode right){
        if(left == null && right == null) return true;
        if(left == null && right != null) return false;
        if(left != null && right == null) return false;
        if(left.val == right.val){
            return isSame(left.left,right.left) && isSame(left.right,right.right);
        }
        return false;
    }
}