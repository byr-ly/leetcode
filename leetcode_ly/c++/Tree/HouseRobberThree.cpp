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
    int rob(TreeNode* root) {
        map<TreeNode*,int> myMap;
        return getResult(root,myMap);
    }
    
    int getResult(TreeNode* root,map<TreeNode*,int>& myMap){
        if(!root) return 0;
        if(myMap.find(root) != myMap.end()) return myMap[root];
        
        int val = 0;
        if(root->left){
            val = val + getResult(root->left->left,myMap) + getResult(root->left->right,myMap);
        }
        if(root->right){
            val = val + getResult(root->right->left,myMap) + getResult(root->right->right,myMap);
        }
        int val1 = getResult(root->left,myMap) + getResult(root->right,myMap);
        int val2 = val + root->val;
        int result = (val2 > val1) ? val2 : val1;
        myMap[root] = result;
        return result;
    }
};