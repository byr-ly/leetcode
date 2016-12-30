class Solution {
public:
    void merge(vector<int>& nums1, int m, vector<int>& nums2, int n) {
        nums1.resize(m + n);
        for(int i = m; i < m + n; i++){
            nums1[i] = nums2[i - m];
        }
        quickSort(nums1,0,m + n -1);
    }
    
    void quickSort(vector<int>& nums,int left,int right){
        int low = left;
        int high = right;
        int key = nums[left];
        if(low < high){
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
            quickSort(nums,left,low - 1);
            quickSort(nums,low + 1,right);
        }
    }
};