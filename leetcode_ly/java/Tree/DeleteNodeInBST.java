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
    public TreeNode deleteNode(TreeNode root, int key) {
        if(root == null) return null;
        if(root.val > key){
            root.left = deleteNode(root.left,key);
        }
        else if(root.val < key){
            root.right = deleteNode(root.right,key);
        }
        else{
            if(root.left != null && root.right != null){
                TreeNode node = getMin(root);
                root.val = node.val;
                root.right = deleteNode(root.right,root.val);
            }
            else if(root.left == null) root = root.right;
            else root = root.left;
        }
        return root;
    }
    
    public TreeNode getMin(TreeNode root){
        TreeNode ptr = root.right;
        while(ptr.left != null){
            ptr = ptr.left;
        }
        return ptr;
    }
}