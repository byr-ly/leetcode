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
    public List<TreeNode> generateTrees(int n) {
        List<TreeNode> res = new ArrayList<>();
        if(n <= 0) return res;
        return buildTree(1,n);
    }
    
    public List<TreeNode> buildTree(int start,int end){
        List<TreeNode> list = new ArrayList<>();
        if(start > end){
            list.add(null);
            return list;
        }
        if(start == end){
            list.add(new TreeNode(start));
            return list;
        }
        
        List<TreeNode> left;
        List<TreeNode> right;
        for(int i = start; i <= end; i++){
            left = buildTree(start, i - 1);
            right = buildTree(i + 1, end);
            for(TreeNode lnode : left){
                for(TreeNode rnode : right){
                    TreeNode root = new TreeNode(i);
                    root.left = lnode;
                    root.right = rnode;
                    list.add(root);
                }
            }
        }
        return list;
    }
}