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
    public List<List<Integer>> zigzagLevelOrder(TreeNode root) {
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        if(root == null) return list;
        
        Queue<TreeNode> q = new LinkedList<TreeNode>();
        q.add(root);
        int cnt = 1;
        while(!q.isEmpty()){
            int len = q.size();
            List<Integer> temp = new ArrayList<Integer>();
            for(int i = 0; i < len; i++){
                TreeNode node = q.poll();
                temp.add(node.val);
                if(node.left != null) q.add(node.left);
                if(node.right != null) q.add(node.right);
            }
            if(cnt % 2 == 0){
                List<Integer> inList = reverse(temp);
                list.add(inList);
            }
            else list.add(temp);
            cnt++;
        }
        return list;
    }
    
    public List<Integer> reverse(List<Integer> temp){
        List<Integer> inList = new ArrayList<Integer>();
        for(int i = temp.size() - 1; i >= 0; i--){
            inList.add(temp.get(i));
        }
        return inList;
    }
}