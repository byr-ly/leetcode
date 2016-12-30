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
    ListNode* rotateRight(ListNode* head, int k) {
        if(head == NULL || head->next == NULL || k == 0){
            return head;
        }
        else{
            int len = getLength(head);
            ListNode* front = head;
            ListNode* end = head;
            k = k % len;
            for(int i = 0; i < k; i++){
                end = end->next;
            }
            if(end == NULL){
                return head;
            }
            else{
                while(end->next){
                    front = front->next;
                    end = end->next;
                }
                end->next = head;
                head = front->next;
                front->next = NULL;
                return head;   
            }
        }
    }
    
    int getLength(ListNode* head){
        int len = 1;
        if(head == NULL){
            return 0;
        }
        else{
            ListNode* front = head;
            while(front->next){
                len++;
                front = front->next;
            }
        }
        return len;
    }
};