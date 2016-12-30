/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
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
    public TreeNode sortedListToBST(ListNode head) {
        return buildTree(head,null);
    }
    
    public TreeNode buildTree(ListNode head,ListNode end){
        if(head == end) return null;
        ListNode slow = head;
        ListNode fast = head;
        //下面这句话注意！！和一般的两指针不一样，终止条件是!= end
        while(fast.next != end && fast.next.next != end){
            slow = slow.next;
            fast = fast.next.next;
        }
        TreeNode root = new TreeNode(slow.val);
        root.left = buildTree(head,slow);
        root.right = buildTree(slow.next,end);
        return root;
    }
}