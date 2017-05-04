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
    HashMap<Integer,Integer> map = new HashMap<>();
    int max = 0;
    public int[] findMode(TreeNode root) {
        if(root == null) return new int[0];
        inorder(root);
        List<Integer> list = new ArrayList<>();
        for(Integer key : map.keySet()){
            if(map.get(key) == max){
                list.add(key);
            }
        }
        int[] res = new int[list.size()];
        for(int i = 0; i < list.size(); i++){
            res[i] = list.get(i);
        }
        return res;
    }
    
    public void inorder(TreeNode root){
        if(root == null) return;
        inorder(root.left);
        int count = map.getOrDefault(root.val,0) + 1;
        max = Math.max(max,count);
        map.put(root.val,count);
        inorder(root.right);
    }
}