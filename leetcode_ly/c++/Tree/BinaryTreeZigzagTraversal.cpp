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
    vector<vector<int>> zigzagLevelOrder(TreeNode* root) {
        vector<vector<int>> result;
        vector<int> ans;
        if(!root) return result;
        
        queue<TreeNode*> q;
        q.push(root);
        int cnt = 1;
        while(!q.empty()){
            int len = q.size();
            for(int i = 0; i < len; i++){
                TreeNode* node = q.front();
                q.pop();
                ans.push_back(node->val);
                if(node->right) q.push(node->right);
                if(node->left) q.push(node->left);
            }
            if(cnt % 2){
                reverse(ans.begin(),ans.end());
            }
            result.push_back(ans);
            ans.clear();
            cnt++;
        }
        return result;
    }
};