class Solution {
public:
    vector<vector<int>> fourSum(vector<int>& nums, int target) {
        int len = nums.size();
        vector<int> init;
        vector<vector<int>> result;
        if(len < 4){
            return result;
        }
        else{
            vector<int> list = quickSort(nums,0,len - 1);
            int first = 0;
            while(first < len){
                int second = first + 1;
                while(second < len){
                    int left = second + 1;
                    int right = len - 1;
                    while(left < right){
                        if(nums[first] + nums[second] + nums[left] + nums[right] == target){
                            init.push_back(nums[first]);
                            init.push_back(nums[second]);
                            init.push_back(nums[left]);
                            init.push_back(nums[right]);
                            if(result.end() == find(result.begin(),result.end(),init)){
                                result.push_back(init);
                            }
                            init.clear();
                            left++;
                            right--;
                        }
                        else if(nums[first] + nums[second] + nums[left] + nums[right] < target){
                            left++;
                        }
                        else{
                            right--;
                        }
                    }
                    second++;
                }
                first++;
            }
            return result;
        }
    }
    
    vector<int> quickSort(vector<int>& nums,int left,int right){
        int low = left;
        int high = right;
        int key = nums[low];
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
        return nums;
    }
};