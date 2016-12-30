class Solution {
public:
    int removeDuplicates(vector<int>& nums) {
        if(nums.size() == 0){
            return 0;
        }
        else{
            int len = nums.size();
            bool flag = true;
            int count = 1;
            for(int i = 1; i < len; i++){
                if(nums[i] == nums[i-1] && flag){
                    nums[count] = nums[i];
                    count++;
                    flag = false;
                }
                else if(nums[i] == nums[i-1] && !flag){
                    continue;
                }
                else{
                    nums[count] = nums[i];
                    count++;
                    flag = true;
                }
            }
            return count;
        }
        
    }
};