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
    public int sumNumbers(TreeNode root) {
        if(root == null) return 0;
        int total = getSum(root,0,0);
        return total;
    }
    
    public int getSum(TreeNode root,int sum,int total){
        if(root == null) return 0;
        sum = sum * 10 + root.val;
        if(root.left == null && root.right == null){
            total += sum;
        }
        return total + getSum(root.left,sum,total) + getSum(root.right,sum,total);
    }
}