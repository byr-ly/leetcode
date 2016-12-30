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
    vector<string> binaryTreePaths(TreeNode* root) {
        vector<string> ans;
        findPath(ans,root,"");
        return ans;
    }
    
    void findPath(vector<string>& ans, TreeNode* root, string s){
        if(root == NULL) return;
        if(root->left == NULL && root->right == NULL){
            ans.push_back(s + to_string(root->val));
            return;
        }
        else{
            findPath(ans,root->left,s + to_string(root->val) + "->");
            findPath(ans,root->right,s + to_string(root->val) + "->");
        }
    }
};