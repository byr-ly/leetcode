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
    void reorderList(ListNode* head) {
        if(!head || !head->next || !head->next->next) return;
        ListNode* node = getMiddleNode(head);
        ListNode* next = node->next;
        node->next = NULL;
        ListNode* back = reverse(next);
        ListNode* Head = head;
        while(back){
            ListNode* temp1 = Head->next;
            ListNode* temp2 = back->next;
            Head->next = back;
            back->next = temp1;
            Head = temp1;
            back = temp2;
        }
    }
    
    ListNode* getMiddleNode(ListNode* head){
        ListNode* slow = head;
        ListNode* fast = head;
        while(fast->next && fast->next->next){
            slow = slow->next;
            fast = fast->next->next;
        }
        return slow;
    }
    
    ListNode* reverse(ListNode* head){
        if(!head || !head->next) return head;
        ListNode* p = NULL;
        ListNode* q = head;
        ListNode* r = head->next;
        while(r){
            q->next = p;
            p = q;
            q = r;
            r = r->next;
        }
        q->next = p;
        return q;
    }
};