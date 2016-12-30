class Solution {
public:
    int wiggleMaxLength(vector<int>& nums) {
        vector<int> ans(nums.size() - 1);
        for(int i = 1; i < nums.size(); i++){
            ans[i - 1] = nums[i] - nums[i - 1];
        }
        
        vector<int> list(nums.size() - 1);
        list[0] = 1;
        for(int i = 1; i < list.size(); i++){
            if(ans[i] * ans[i - 1] < 0) list[i] = list[i - 1] + 1;
            else list[i] = list[i - 1];
        }
        return list[list.size() - 1] + 1;
    }
};

class Solution {
public:
    int wiggleMaxLength(vector<int>& nums) {
        int len = nums.size();
        if(len <= 1) return len;
        int ans = len;
        int flag = 0;
        for(int i = 1; i < len; i++){
            if(nums[i] == nums[i - 1]) ans--;
            else if(nums[i] > nums[i - 1]) flag == 1 ? ans-- : flag = 1;
            else flag == -1 ? ans-- : flag = -1;
        }
        return ans;
    }
};