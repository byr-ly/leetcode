/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) {
 *         val = x;
 *         next = null;
 *     }
 * }
 */
public class Solution {
    public ListNode getIntersectionNode(ListNode headA, ListNode headB) {
        int ca = 0; 
        int cb = 0; 
        ListNode copyA = headA; 
        ListNode copyB = headB; 
        while(copyA != null){ 
            copyA = copyA.next; 
            ca++; 
        } 
        while(copyB != null){ 
            copyB = copyB.next; 
            cb++; 
        } 

        if(ca > cb) return getIntersectionNode(headB,headA); 
        int diff = cb - ca; 
        while(diff != 0){ 
            headB = headB.next; 
            diff--; 
        } 
        while(headA != headB){ 
            headA = headA.next; 
            headB = headB.next; 
        } 
        return headA; 
    }
}