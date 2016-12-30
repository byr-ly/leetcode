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
    public boolean isValidBST(TreeNode root) {
        //看到二分查找树就要想到中序遍历
        List<Integer> list = new ArrayList<Integer>();
        inorder(root,list);
        for(int i = 1; i < list.size(); i++){
            if(list.get(i) <= list.get(i - 1)) return false;
        }
        return true;
    }
    
    public void inorder(TreeNode root,List<Integer> list){
        if(root == null) return;
        inorder(root.left,list);
        list.add(root.val);
        inorder(root.right,list);
        return;
    }
}