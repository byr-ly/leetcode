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
    ListNode* insertionSortList(ListNode* head) {
        if(!head || !head->next) return head;
        ListNode* node = new ListNode(INT_MIN);
        node->next = NULL;
        while(head){
            ListNode* tail = head->next;
            int index = getIndex(node,head);
            ListNode* front = node;
            for(int i = 0; i < index - 1; i++){
                front = front->next;
            }
            head->next = front->next;
            front->next = head;
            head = tail;
        }
        return node->next;
    }
    
    int getIndex(ListNode* node,ListNode* head){
        int index = 0;
        while(node){
            if(node->val < head->val){
                index++;
            }
            node = node->next;
        }
        return index;
    }
};