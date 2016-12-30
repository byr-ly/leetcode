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
    ListNode* reverseBetween(ListNode* head, int m, int n) {
        if(!head || !head->next || m == n) return head;
        ListNode* front = new ListNode(0);
        front->next = head;
        ListNode* low = front;
        if(m != 1){
            for(int i = 0; i < m - 1; i++){
                low = low->next;
            }
        }
        ListNode* q = low->next;
        ListNode* node = q;
        low->next = NULL;
        ListNode* p = NULL;
        ListNode* r = q->next;
        int cnt = 1;
        while(r && cnt <= n - m){
            q->next = p;
            p = q;
            q = r;
            r = r->next;
            cnt++;
        }
        q->next = p;
        low->next = q;
        node->next = r;
        return front->next;
    }
};