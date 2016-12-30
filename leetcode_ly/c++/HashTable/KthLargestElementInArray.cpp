class Solution {
public:
    int findKthLargest(vector<int>& nums, int k) {
        priority_queue<int> hash;
        for(int i = 0; i < nums.size(); i++){
            hash.push(nums[i]);
        }
        int j = 1;
        while(j < k){
            hash.pop();
            j++;
        }
        return hash.top();
    }
};