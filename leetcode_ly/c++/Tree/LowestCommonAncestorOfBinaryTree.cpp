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
    TreeNode* lowestCommonAncestor(TreeNode* root, TreeNode* p, TreeNode* q) {
        if(!root || !p || !q) return NULL;
        vector<TreeNode*> pPath;
        vector<TreeNode*> qPath;
        
        pPath.push_back(root);
        qPath.push_back(root);
        
        TreeNode* node = NULL;
        if(getPath(root,p,pPath) && getPath(root,q,qPath)){
            for(int i = 0; i < pPath.size() && i < qPath.size(); i++){
                if(pPath[i] == qPath[i]) node = pPath[i];
                else break;
            }
        }
        return node;
    }
    
    bool getPath(TreeNode* root,TreeNode* p,vector<TreeNode*>& pPath){
        if(root == p) return true;
        
        if(root->left){
            pPath.push_back(root->left);
            if(getPath(root->left,p,pPath)) return true;
            pPath.pop_back();
        }
        
        if(root->right){
            pPath.push_back(root->right);
            if(getPath(root->right,p,pPath)) return true;
            pPath.pop_back();
        }
        
        return false;
    }
};