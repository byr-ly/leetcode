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
    public void flatten(TreeNode root) {
        if(root == null) return;
        while(root.right != null && root.left == null) root = root.right;
        while(root.left != null){
            TreeNode node = root.left;
            TreeNode ptr = root.right;
            root.right = node;
            root.left = null;
            while(node.right != null){
                node = node.right;
            }
            node.right = ptr;
            while(root.right != null && root.left == null) root = root.right;
        }
        return;
    }
}