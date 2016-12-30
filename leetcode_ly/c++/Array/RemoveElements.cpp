class Solution {
public:
    int removeElement(vector<int>& nums, int val) {
        if(nums.size() == 0){
            return 0;
        }
        else{
            int len = nums.size();
            int count = 0;
            for(int i = 0; i < len; i++){
                if(nums[i] == val){
                    continue;
                }
                else{
                    nums[count] = nums[i];
                    count++;
                }
            }
            return count;
        }
    }
};