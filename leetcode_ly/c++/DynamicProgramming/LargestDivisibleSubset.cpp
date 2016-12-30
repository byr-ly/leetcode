class Solution {
public:
    vector<int> largestDivisibleSubset(vector<int>& nums) {
        int n = nums.size();
        vector<int> ans;
        if(n == 0) return ans;
        vector<int> res(n,1);
        vector<int> index(n,-1);
        quickSort(nums,0,nums.size() - 1);
        
        int max = 0; 
        int idx = 0;
        for(int i = 1; i < n; i++){
            for(int j = 0; j < i; j++){
                if(nums[i] % nums[j] == 0){
                    if(res[i] < res[j] + 1){
                        res[i] = res[j] + 1;
                        index[i] = j;
                    }
                }                
            }
            if(max < res[i]){
                max = res[i];
                idx = i;
            }
        }
        
        while(index[idx] != -1){
            ans.push_back(nums[idx]);
            idx = index[idx];
        }
        ans.push_back(nums[idx]);
        return ans;
    }
    
    void quickSort(vector<int>& nums,int left,int right){
        int low = left;
        int high = right;
        int target = nums[low];
        if(low < high){
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


