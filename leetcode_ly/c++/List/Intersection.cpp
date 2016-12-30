/**
 * Definition for singly-linked list.
 * struct ListNode {
 *     int val;
 *     ListNode *next;
 *     ListNode(int x) : val(x), next(NULL) {}
 * };
 */
class Solution {
public:
    ListNode *getIntersectionNode(ListNode *headA, ListNode *headB) {
        int lenA = getLength(headA);
        int lenB = getLength(headB);
        if(lenA >= lenB){
            int index = lenA - lenB;
            while(index != 0){
                headA = headA->next;
                index--;
            }
            while(headA != headB && headA){
                headA = headA->next;
                headB = headB->next;
            }
            if(headA == NULL){
                return NULL;
            }
            return headA;
        }
        else if(lenA < lenB){
            int index = lenB - lenA;
            while(index != 0){
                headB = headB->next;
                index--;
            }
            while(headA != headB && headB){
                headA = headA->next;
                headB = headB->next;
            }
            if(headB == NULL){
                return NULL;
            }
            return headB;
        }
        return NULL;
    }
    
    int getLength(ListNode* head){
        int cnt = 0;
        while(head){
            cnt += 1;
            head = head->next;
        }
        return cnt;
    }
};