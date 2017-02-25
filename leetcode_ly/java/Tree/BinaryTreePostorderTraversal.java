/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
 //
public class Solution {
    public List<Integer> postorderTraversal(TreeNode root) {
        List<Integer> list =  new ArrayList<Integer>();
        if(root == null) return list;
        postorder(root,list);
        return list;
    }
    
    public void postorder(TreeNode root,List<Integer> list){
        if(root == null) return;
        postorder(root.left,list);
        postorder(root.right,list);
        list.add(root.val);
        return;
    }
}

/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
 //
public class Solution {
    public List<Integer> postorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<Integer>();
        Stack<TreeNode> s = new Stack<TreeNode>();
        if(root == null) return res;
        postorder(root,s,res);
        return res;
    }
    
    public void postorder(TreeNode root,Stack<TreeNode> s,List<Integer> res){
        s.push(root);
        TreeNode pre = null;
        while(!s.isEmpty()){
            root = s.peek();
            if((root.left == null && root.right == null) || (pre != null && (root.left == pre || root.right == pre))){
                res.add(root.val);
                s.pop();
                pre = root;
            }
            else{
                if(root.right != null) s.push(root.right);
                if(root.left != null) s.push(root.left);
            }
        }
    }
}