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
    public TreeNode buildTree(int[] preorder, int[] inorder) {
        if(preorder.length == 0) return null;
        int n = preorder.length;
        return buildTree(preorder,0,n - 1,inorder,0,n - 1);
    }
    
    public TreeNode buildTree(int[] preorder,int left,int right,int[] inorder,int low,int high){
        if(left <= right){
            int rootValue = preorder[left];
            int i;
            for(i = low; i <= high; i++){
                if(inorder[i] == rootValue) break;
            }
            int leftSize = i - low;
            TreeNode root = new TreeNode(rootValue);
            root.left = buildTree(preorder,left + 1,left + leftSize,inorder,i - leftSize,i - 1);
            root.right = buildTree(preorder,left + leftSize + 1,right,inorder,i + 1,high);
            return root;
        }
        return null;
    }
}