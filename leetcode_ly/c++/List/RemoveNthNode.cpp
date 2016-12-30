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
    ListNode* removeNthFromEnd(ListNode* head, int n) {
        if(head == NULL ||  n == 0){
            return head;
        }
        else if(head->next == NULL && n == 1){
            return NULL;
        }
        else{
            ListNode* front = head;
            ListNode* end = head;
            for(int i = 0 ; i < n - 1; i++){
                end = end->next;
            }
            if(end->next == NULL){
                head = head->next;
                return head;
            }
            else{
                ListNode* copy = head;
                while(end->next){
                    copy = front;
                    front = front->next;
                    end = end->next;
                }
                copy->next = copy->next->next;
            }
            return head;
        }
    }
};