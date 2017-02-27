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
    public List<Integer> preorderTraversal(TreeNode root) {
        List<Integer> list = new ArrayList<Integer>();
        if(root == null) return list;
        preorder(root,list);
        return list;
    }
    
    public void preorder(TreeNode root,List<Integer> list){
        if(root == null) return;
        list.add(root.val);
        preorder(root.left,list);
        preorder(root.right,list);
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
//O(N)
public class Solution {
    public List<Integer> preorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<Integer>();
        Stack<TreeNode> s = new Stack<TreeNode>();
        if(root == null) return res;
        preorder(root,s,res);
        return res;
    }
    
    public void preorder(TreeNode root,Stack<TreeNode> s,List<Integer> res){
        while(root != null || !s.isEmpty()){
            res.add(root.val);
            s.push(root);
            root = root.left;
            while(root == null && !s.isEmpty()){
                root = s.pop();
                root = root.right;
            }
        }
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
 //O(1)
public class Solution {
    public List<Integer> preorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<Integer>();
        if(root == null) return res;
        preorder(root,res);
        return res;
    }
    
    public void preorder(TreeNode root,List<Integer> res){
        TreeNode cur = root;
        TreeNode pre = null;
        while(cur != null){
            if(cur.left == null){
                res.add(cur.val);
                cur = cur.right;
            }
            else{
                pre = cur.left;
                while(pre.right != null && pre.right != cur){
                    pre = pre.right;
                }
                if(pre.right == null){
                    res.add(cur.val);// the only difference with inorder-traversal
                    pre.right = cur;
                    cur = cur.left;
                }
                else{
                    pre.right = null;
                    cur = cur.right;
                }
            }
        }
    }
}