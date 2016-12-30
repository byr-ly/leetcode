class Solution {
public:
    void rotate(vector<int>& nums, int k) {
        int len = nums.size();
        if(k <= 0){
            return;
        }
        else{
            k = k % len;
            nums.resize(len + len - k);
            for(int i = len; i < nums.size(); i++){
                nums[i] = nums[i - len];
            }
            for(int j = 0; j < len; j++){
                nums[j] = nums[j + len - k];
            }
            nums.resize(len);   
        }
    }
};