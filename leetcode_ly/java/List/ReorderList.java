/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public void reorderList(ListNode head) {
        if(head == null || head.next == null) return;
        ListNode slow = head;
        ListNode fast = head;
        while(fast.next != null && fast.next.next != null){
            slow = slow.next;
            fast = fast.next.next;
        }
        ListNode temp = slow.next;
        slow.next = null;
        ListNode node = reverse(temp);
        //������仰�ĳ�ʼ�������壬��Ϊ�˱���node��node���ȱ�head��ʱ����βָ�벹��
        ListNode tail = node;
        while(node != null && head != null){
            tail = node;
            ListNode ptr = head.next;
            ListNode btr = node.next;
            head.next = node;
            node.next = ptr;
            head = ptr;
            node = btr;
        }
        if(head == null && node != null) tail.next = node;
    }
    
    public ListNode reverse(ListNode head){
        ListNode p = null;
        ListNode q = head;
        while(head.next != null){
            head = head.next;
            q.next = p;
            p = q;
            q = head;
        }
        head.next = p;
        return head;
    }
}