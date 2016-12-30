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

    // Encodes a tree to a single string.
    public String serialize(TreeNode root) {
        StringBuffer s = new StringBuffer();
        if(root == null) return s.toString();
        preorder(s,root);
        return s.substring(0,s.length() - 1);
    }
    
    public void preorder(StringBuffer s,TreeNode root){
        if(root == null) return;
        s = s.append(root.val).append('#');
        preorder(s,root.left);
        preorder(s,root.right);
        return;
    }

    // Decodes your encoded data to tree.
    public TreeNode deserialize(String data) {
        if(data.isEmpty()) return null;
        String[] line = data.split("#");
        return buildTree(line,0,line.length - 1);
    }
    
    public TreeNode buildTree(String[] line,int low,int high){
        if(low > high) return null;
        int value = Integer.parseInt(line[low]);
        TreeNode root = new TreeNode(value);
        int index = getIndex(line,value,low + 1,high);
        root.left = buildTree(line,low + 1,index - 1);
        root.right = buildTree(line,index,high);
        return root;
    }
    
    public int getIndex(String[] line,int value,int low,int high){
        int i = low;
        for(; i <= high; i++){
            if(Integer.parseInt(line[i]) > value) break;
        }
        return i;
    }
}

// Your Codec object will be instantiated and called as such:
// Codec codec = new Codec();
// codec.deserialize(codec.serialize(root));