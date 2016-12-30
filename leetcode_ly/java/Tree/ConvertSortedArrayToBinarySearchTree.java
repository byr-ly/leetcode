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
    public TreeNode sortedArrayToBST(int[] nums) {
        int len = nums.length;
        if(len == 0) return null;
        return buildTree(nums,0,len - 1);
    }
    
    public TreeNode buildTree(int[] nums,int start,int end){
        if(start > end) return null;
        int middle = start + (end - start) / 2;
        TreeNode root = new TreeNode(nums[middle]);
        root.left = buildTree(nums,start,middle - 1);
        root.right = buildTree(nums,middle + 1,end);
        return root;
    }
}