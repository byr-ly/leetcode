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
    int tilt = 0;
    public int findTilt(TreeNode root) {
        postorder(root);
        return tilt;
    }
    
    public int postorder(TreeNode root){
        if(root == null) return 0;
        int leftNum = postorder(root.left);
        int rightNum = postorder(root.right);
        tilt += Math.abs(leftNum - rightNum);
        return root.val + leftNum + rightNum;
    }
}