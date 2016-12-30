class Solution {
public:
    Solution(vector<int> nums) {
        result = nums;
    }
    
    /** Resets the array to its original configuration and return it. */
    vector<int> reset() {
        return result;
    }
    
    /** Returns a random shuffling of the array. */
    vector<int> shuffle() {
        vector<int> ans(result);
        for(int i = ans.size() - 1; i >= 0; i--){
            int j = rand() % (i + 1);
            int temp = ans[i];
            ans[i] = ans[j];
            ans[j] = temp;
        }
        return ans;
    }

private:
    vector<int> result;
};

/**
 * Your Solution object will be instantiated and called as such:
 * Solution obj = new Solution(nums);
 * vector<int> param_1 = obj.reset();
 * vector<int> param_2 = obj.shuffle();
 */