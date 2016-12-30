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
    public List<List<Integer>> pathSum(TreeNode root, int sum) {
        List<List<Integer>> list = new ArrayList<List<Integer>>();
        if(root == null) return list;
        List<Integer> inList = new ArrayList<Integer>();
        getPath(root,sum,list,inList);
        return list;
    }
    
    public void getPath(TreeNode root,int sum,List<List<Integer>> list,List<Integer> inList){
        if(root == null) return;
        inList.add(root.val);
        if(root.left == null && root.right == null && root.val == sum){
            list.add(new ArrayList<Integer>(inList));
        }
        
        getPath(root.left,sum - root.val,list,inList);
        getPath(root.right,sum - root.val,list,inList);
        inList.remove(inList.size() - 1);
        return;
    }
}