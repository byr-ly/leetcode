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
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> list = new ArrayList<Integer>();
        if(root == null) return list;
        inorder(root,list);
        return list;
    }
    
    public void inorder(TreeNode root,List<Integer> list){
        if(root == null) return;
        inorder(root.left,list);
        list.add(root.val);
        inorder(root.right,list);
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
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<Integer>();
        Stack<TreeNode> s = new Stack<TreeNode>();
        if(root == null) return res;
        inorder(root,s,res);
        return res;
    }
    
    public void inorder(TreeNode root,Stack<TreeNode> s,List<Integer> res){
        while(root != null || !s.isEmpty()){
            s.push(root);
            root = root.left;
            while(root == null && !s.isEmpty()){
                root = s.pop();
                res.add(root.val);
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
 //O(1)
public class Solution {
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> res = new ArrayList<Integer>();
        if(root == null) return res;
        inorder(root,res);
        return res;
    }
    
    public void inorder(TreeNode root,List<Integer> res){
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
                    pre.right = cur;
                    cur = cur.left;
                }
                else{
                    pre.right = null;
                    res.add(cur.val);
                    cur = cur.right;
                }
            }
        }
    }
}