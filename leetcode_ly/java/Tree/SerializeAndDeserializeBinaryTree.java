/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */
public class Codec {
		//前序遍历和中序遍历唯一确定二叉树需要不能有重复元素

    // Encodes a tree to a single string.
    public String serialize(TreeNode root) {
        StringBuffer s = new StringBuffer();
        order(root,s);
        return s.substring(0,s.length() - 1);
    }
    
    public void order(TreeNode root,StringBuffer s){
        if(root == null) s.append("X").append(",");
        else{
            s.append(root.val).append(",");
            order(root.left,s);
            order(root.right,s);
        }
    }

    // Decodes your encoded data to tree.
    public TreeNode deserialize(String data) {
        Queue<String> q = new LinkedList<String>();
        q.addAll(Arrays.asList(data.split(",")));
        return buildTree(q);
    }
    
    public TreeNode buildTree(Queue<String> q){
        String s = q.poll();
        if(s.equals("X")) return null;
        else{
            TreeNode root = new TreeNode(Integer.parseInt(s));
            root.left = buildTree(q);
            root.right = buildTree(q);
            return root;
        }
    }
}

// Your Codec object will be instantiated and called as such:
// Codec codec = new Codec();
// codec.deserialize(codec.serialize(root));