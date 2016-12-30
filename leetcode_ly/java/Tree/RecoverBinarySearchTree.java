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
    public void recoverTree(TreeNode root) {
        List<TreeNode> list = new ArrayList<TreeNode>();
        inorder(root,list);
        
        TreeNode ptr = null;
        TreeNode ptr1 = null;
        int cnt = 1;
        for(int i = 1; i < list.size(); i++){
            if(cnt == 2 && list.get(i).val < list.get(i - 1).val){
                ptr1 = list.get(i);
            }
            if(cnt == 1 && list.get(i).val < list.get(i - 1).val){
                ptr = list.get(i - 1);
                ptr1 = list.get(i);
                cnt++;
            }
        }
        
        int temp = ptr.val;
        ptr.val = ptr1.val;
        ptr1.val = temp;
    }
    
    public void inorder(TreeNode root,List<TreeNode> list){
        if(root == null) return;
        inorder(root.left,list);
        list.add(root);
        inorder(root.right,list);
        return;
    }
}