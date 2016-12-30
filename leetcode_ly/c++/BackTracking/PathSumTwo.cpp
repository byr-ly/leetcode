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
    vector<vector<int>> pathSum(TreeNode* root, int sum) {
        vector<vector<int>> result;
        vector<int> ans;
        getResult(root,result,ans,sum);
        return result;
    }
    
    void getResult(TreeNode* root,vector<vector<int>>& result,vector<int>& ans,int sum){
        if(!root) return;
        ans.push_back(root->val);
        if(!root->left && !root->right && sum == root->val){
            result.push_back(ans);
            //return;
        }

        getResult(root->left,result,ans,sum - root->val);
        getResult(root->right,result,ans,sum - root->val);
        ans.pop_back();
        return;
    }
};