class Solution {
public:
    vector<int> singleNumber(vector<int>& nums) {
        int result = 0;
        for(int i = 0; i < nums.size(); i++){
            result ^= nums[i];
        }
        int pos = result & ~(result - 1);
        vector<int> ret(2,0);
        for(int j = 0; j < nums.size(); j++){
            if(pos & nums[j]){
                ret[0] ^= nums[j];
            }
            else{
                ret[1] ^= nums[j];
            }
        }
        return ret;
    }
};