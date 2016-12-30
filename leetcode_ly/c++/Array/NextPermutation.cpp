class Solution {
public:
    void nextPermutation(vector<int>& nums) {
        int len = nums.size();
        int i;
        // 1.从后往前，找到第一个 A[i-1] < A[i]的。2,4,6,5,3,1 可以看到A[i]到A[n-1]这些都是单调递减序列。
        for(i = len - 1; i >= 0; i--){
            if(nums[i] <= nums[i - 1]) continue;
            else break;
        }
        if(i == 0){
            sort(nums.begin(),nums.end());
            return;
        }
        //2.从 A[n-1]到A[i]中找到一个比A[i-1]大的值（也就是说在A[n-1]到A[i]的值中找到比A[i-1]大的集合中的最小的一个值）
        int index;
        int j;
        for(j = i; j < len; j++){
            if(nums[j] > nums[i - 1]) continue;
            else{
                index = j - 1;
                break;
            }
        }
        if(j == len) index = j - 1;
        	//3.交换 这两个值，并且把A[n-1]到A[i+1]排序，从小到大。
        int temp;
        temp = nums[i - 1];
        nums[i - 1] = nums[index];
        nums[index] = temp;
        sort(nums.begin() + i,nums.end());
    }
};