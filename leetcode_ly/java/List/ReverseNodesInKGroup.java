/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode reverseKGroup(ListNode head, int k) {
        ListNode ptr = new ListNode(0);
        ptr.next = head;
        ListNode ptr1 = ptr;
        ListNode curNode = head;
        int cnt = 0;
        while(curNode != null){
            ListNode left = head;
            cnt++;
            if(cnt == k){
                ListNode tail = curNode.next;
                curNode.next = null;
                ListNode leftCopy = left;
                reverse(ptr1,left,curNode);
                leftCopy.next = tail;
                curNode = tail;
                head = curNode;
                ptr1 = leftCopy;
                cnt = 0;
                continue;
            }
            curNode = curNode.next;
        }
        return ptr.next;
    }
    
    public void reverse(ListNode ptr1,ListNode left,ListNode right){
        ListNode p = null;
        ListNode q = left;
        while(left != right){
            left = left.next;
            q.next = p;
            p = q;
            q = left;
        }
        left.next = p;
        ptr1.next = left;
    }
}