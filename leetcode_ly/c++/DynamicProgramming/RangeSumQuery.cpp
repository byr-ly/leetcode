class NumArray {
public:
    NumArray(vector<int> &nums) {
        if(nums.size() == 0) return;
        int res = 0;
        for(int i = 0; i < nums.size(); i++){
            res += nums[i];
            sums.push_back(res);
        }
    }

    int sumRange(int i, int j) {
        return sums[j] - sums[i - 1];
    }
    
    private:
        vector<int> sums;
};


// Your NumArray object will be instantiated and called as such:
// NumArray numArray(nums);
// numArray.sumRange(0, 1);
// numArray.sumRange(1, 2);