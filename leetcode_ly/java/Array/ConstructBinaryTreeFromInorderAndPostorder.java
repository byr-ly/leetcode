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
    public TreeNode buildTree(int[] inorder, int[] postorder) {
        if(postorder.length == 0) return null;
        int n = postorder.length;
        return buildTree(postorder,0,n - 1,inorder,0,n - 1);
    }
    
    public TreeNode buildTree(int[] postorder,int left,int right,int[] inorder,int low,int high){
        if(left > right) return null;
        int root = postorder[right];
        int i;
        for(i = low; i <= high; i++){
            if(inorder[i] == root) break;
        }
        int leftSize = i - low;
        TreeNode node = new TreeNode(root);
        node.left = buildTree(postorder,left,left + leftSize - 1,inorder,low,i - 1);
        node.right = buildTree(postorder,left + leftSize,right - 1,inorder,i + 1,high);
        return node;
    }
}