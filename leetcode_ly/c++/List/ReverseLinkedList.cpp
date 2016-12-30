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
    ListNode* reverseList(ListNode* head) {
        if(head == NULL || head->next == NULL){
            return head;
        }
        else{
            ListNode* p = head;
            ListNode* q = head->next;
            ListNode* r;
            head->next = NULL;
            while(q->next){
                r = q->next;
                q->next = p;
                p = q;
                q = r;
            }
            q->next = p;
            head = q;
            return head;
        }
    }
};