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
    int sumNumbers(TreeNode* root) {
        return getResult(root,0);
    }
    
    int getResult(TreeNode* root,int sum){
        if(!root) return 0;
        if(!root->left && !root->right) return sum * 10 + root->val;
        else return getResult(root->left,sum * 10 + root->val) + getResult(root->right,sum * 10 + root->val);
    }
};