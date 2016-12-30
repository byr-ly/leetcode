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
    ListNode* addTwoNumbers(ListNode* l1, ListNode* l2) {
        ListNode* front = l1;
        ListNode* end = l2;
        ListNode* frontCopy;
        ListNode* endCopy;
        int addition = 0;
        while(front != NULL && end != NULL){
            frontCopy = front;
            endCopy = end;
            front->val = front->val + end->val + addition;
            addition = 0;
            if(front->val >= 10){
                addition = 1;
                front->val = front->val % 10;
            }
            front = front->next;
            end = end->next;
        }
        if(front == NULL && end != NULL){
            frontCopy->next = end;
            end->val = end->val + addition;
            while(end->val >= 10){
                frontCopy = end;
                addition = 1;
                end->val = end->val % 10;
                if(end->next){
                    end = end->next;
                    end->val = end->val + addition;
                }
                else{
                    ListNode* temp = new ListNode(1);
                    frontCopy->next = temp;
                }
            }
        }
        else if(end == NULL && front != NULL){
            frontCopy->next = front;
            front->val = front->val + addition;
            while(front && front->val >= 10){
                frontCopy = front;
                addition = 1;
                front->val = front->val % 10;
                if(front->next){
                    front = front->next;
                    front->val = front->val + addition;
                }
                else{
                    ListNode* temp = new ListNode(1);
                    frontCopy->next = temp;
                }
            }
        }
        else if(end == NULL && front == NULL && addition == 1){
            ListNode* temp = new ListNode(1);
            frontCopy->next = temp;
        }
        return l1;
    }
};