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
    public int countNodes(TreeNode root) {
        if(root == null) return 0;
        int l = getLeft(root);
        int r = getRight(root);
        
        if(l == r) return (2 << (l - 1)) - 1;
        else return countNodes(root.left) + countNodes(root.right) + 1;
    }
    
    public int getLeft(TreeNode root){
        int cnt = 0;
        while(root != null){
            cnt++;
            root = root.left;
        }
        return cnt;
    }
    
    public int getRight(TreeNode root){
        int cnt = 0;
        while(root != null){
            cnt++;
            root = root.right;
        }
        return cnt;
    }
}