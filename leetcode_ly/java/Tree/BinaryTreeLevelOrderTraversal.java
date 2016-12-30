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
    public List<List<Integer>> levelOrder(TreeNode root) {
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        Queue<TreeNode> q = new LinkedList<TreeNode>();
        if(root == null) return list;
        q.add(root);
        while(!q.isEmpty()){
            int len = q.size();
            ArrayList<Integer> in = new ArrayList<Integer>();
            for(int i = 0; i < len; i++){
                TreeNode node = q.poll();
                in.add(node.val);
                if(node.left != null) q.add(node.left);
                if(node.right != null) q.add(node.right);
            }
            list.add(in);
        }
        return list;
    }
}