/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode(int x) : val(x), left(NULL), right(NULL) {}
 * };
 */
class Solution {
public:
    vector<vector<int>> levelOrderBottom(TreeNode* root) {
        vector<vector<int>> outvec;
        vector<int> invec;
        if(!root) return outvec;
        queue<TreeNode*> q1;
        q1.push(root);
        while(!q1.empty()){
            queue<TreeNode*> q2;
            while(!q1.empty()){
                invec.push_back(q1.front()->val);
                if(q1.front()->left) q2.push(q1.front()->left);
                if(q1.front()->right) q2.push(q1.front()->right);
                q1.pop();
            }
            q1= q2;
            outvec.push_back(invec);
            invec.clear();
        }
        // vector<vector<int>> result;
        // vector<vector<int>>::reverse_iterator it;
        // for(it = outvec.rbegin(); it != outvec.rend(); it++){
        //     result.push_back(*it);
        // }
        // return result;
        reverse(outvec.begin(),outvec.end());
        return outvec;
    }
};