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
    public List<List<Integer>> levelOrderBottom(TreeNode root) {
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        if(root == null) return list;
        Queue<TreeNode> q = new LinkedList<TreeNode>();
        q.add(root);
        
        while(!q.isEmpty()){
            int len = q.size();
            ArrayList<Integer> temp = new ArrayList<Integer>();
            for(int i = 0; i < len; i++){
                TreeNode node = q.poll();
                temp.add(node.val);
                if(node.left != null) q.add(node.left);
                if(node.right != null) q.add(node.right);
            }
            list.add(temp);
        }
        
        List<List<Integer>> result = new ArrayList<List<Integer>>();
        for(int i = list.size() - 1; i >= 0; i--){
            result.add(list.get(i));
        }
        return result;
    }
}