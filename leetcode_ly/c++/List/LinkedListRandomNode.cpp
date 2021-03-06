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
    /** @param head The linked list's head.
        Note that the head is guaranteed to be not null, so it contains at least one node. */
    Solution(ListNode* head) {
        ListNode* p = head;
        ListNode* node = head;
        while(node){
            result.push_back(node->val);
            node = node->next;
        }
    }
    
    /** Returns a random node's value. */
    int getRandom() {
        return result[rand() % result.size()];
    }
    
private:
    ListNode* p;
    vector<int> result;
};

/**
 * Your Solution object will be instantiated and called as such:
 * Solution obj = new Solution(head);
 * int param_1 = obj.getRandom();
 */