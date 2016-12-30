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
      public TreeNode lowestCommonAncestor(TreeNode root, TreeNode p, TreeNode q) {
        if(root == null || p == null || q == null) return null;
        List<TreeNode> pPath = new ArrayList<>();
        List<TreeNode> qPath = new ArrayList<>();
        
        // pPath.add(root);  
        // qPath.add(root); 
        //��������ʼ��Ϊroot,������ʹp,q��������ͬ��֧Ҳ��ֱ�ӷ���ͷ���
        TreeNode node = root;
        if(getPath(root,p,pPath) && getPath(root,q,qPath)){
            for(int i = 0; i < pPath.size() && i < qPath.size(); i++){
                if(pPath.get(i) == qPath.get(i)) node = pPath.get(i);
                else break;
            }
        }
        return node;
    } 
    
    //��Ϊ��������ִ��add������List��������Լ�ڴ棬ͬʱҲ��ʵ�ַ���
    public boolean getPath(TreeNode root,TreeNode p,List<TreeNode> pPath){
        if(root == p) return true;
        if(root.left != null){
            pPath.add(root.left);
            if(getPath(root.left,p,pPath)) return true;
            pPath.remove(pPath.size() - 1);
        }
        
        if(root.right != null){
            pPath.add(root.right);
            if(getPath(root.right,p,pPath)) return true;
            pPath.remove(pPath.size() - 1);
        }
        return false;
    }
}