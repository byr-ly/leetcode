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
    ListNode* sortList(ListNode* head) {
        if(!head || !head->next) return head;
        ListNode* node = getMiddleNode(head);
        ListNode* next = node->next;
        node->next = NULL;
        ListNode* result = merge(sortList(head),sortList(next));
        return result;
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
    
    ListNode* merge(ListNode* a,ListNode* b){
        ListNode* front = new ListNode(-1);
        ListNode* cur = front;
        while(a && b){
            if(a->val <= b->val){
                cur->next = a;
                a = a->next;
            }
            else{
                cur->next = b;
                b = b->next;
            }
            cur = cur->next;
        }
        cur->next = (a) ? a : b;
        return front->next;
    }
};