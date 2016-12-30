/**
 * Definition for binary tree with next pointer.
 * public class TreeLinkNode {
 *     int val;
 *     TreeLinkNode left, right, next;
 *     TreeLinkNode(int x) { val = x; }
 * }
 */
public class Solution {
    public void connect(TreeLinkNode root) {
        if(root == null) return;
        Queue<TreeLinkNode> q = new LinkedList<TreeLinkNode>();
        q.add(root);
        while(!q.isEmpty()){
            int len = q.size();
            for(int i = 0; i < len - 1; i++){
                TreeLinkNode node = q.poll();
                if(node.left != null) q.add(node.left);
                if(node.right != null) q.add(node.right);
                node.next = q.peek();
            }
            TreeLinkNode ptr = q.poll();
            if(ptr.left != null) q.add(ptr.left);
            if(ptr.right != null) q.add(ptr.right);
            ptr.next = null;
        }
        return;
    }
}