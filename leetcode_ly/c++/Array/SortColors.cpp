class Solution {
public:
    void sortColors(vector<int>& nums) {
        quickSort(0,nums.size() - 1,nums);
    }
    
    void quickSort(int left,int right,vector<int>& nums){
        if(left < right){
            int low = left;
            int high = right;
            int key = nums[low];
            while(low < high){
                while(low < high && nums[high] >= key){
                    high--;
                }
                nums[low] = nums[high];
                while(low < high && nums[low] <= key){
                    low++;
                }
                nums[high] = nums[low];
            }
            nums[low] = key;
            quickSort(low + 1, right,nums);
            quickSort(left,low - 1,nums);
        }
    }
};