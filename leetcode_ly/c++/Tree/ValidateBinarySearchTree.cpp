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
    vector<int> ret;

    bool isValidBST(TreeNode* root) {
        inorder(root);
        for(int i = 1; i < ret.size(); i++){
            if(ret[i] <= ret[i - 1]) return false;
            else continue;
        }
        return true;
    }
    
    void inorder(TreeNode* root){
        if(!root) return;
        inorder(root->left);
        ret.push_back(root->val);
        inorder(root->right);
    }
};