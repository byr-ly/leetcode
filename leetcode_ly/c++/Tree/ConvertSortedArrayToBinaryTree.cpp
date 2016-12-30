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
    TreeNode* sortedArrayToBST(vector<int>& nums) {
        return buildTree(nums,0,nums.size() - 1);
    }
    
    TreeNode* buildTree(vector<int>& nums,int begin,int end){
        if(begin > end) return NULL;
        int middle = begin + (end - begin) / 2;
        TreeNode* root = new TreeNode(nums[middle]);
        root->left = buildTree(nums,begin,middle - 1);
        root->right = buildTree(nums,middle + 1,end);
        return root;
    }
};