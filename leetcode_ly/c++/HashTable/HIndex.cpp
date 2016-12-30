class Solution {
public:
    int hIndex(vector<int>& citations) {
        int len = citations.size();
        if(len == 0) return 0;
        
        quickSort(citations,0,len - 1);
        int num = len - 1;
        int i = 1;
        while(num >= 0){
            if(citations[num] >= i){
                i++;
                num--;
            }
            else break;
        }
        return i - 1;
    }
    
    void quickSort(vector<int>& nums,int left,int right){
        if(left < right){
            int low = left;
            int high = right;
            int target = nums[low];
            while(low < high){
                while(low < high && nums[high] >= target){
                    high--;
                }
                nums[low] = nums[high];
                while(low < high && nums[low] <= target){
                    low++;
                }
                nums[high] = nums[low];
            }
            nums[low] = target;
            quickSort(nums,left,low - 1);
            quickSort(nums,low + 1,right);
        }
    }
};