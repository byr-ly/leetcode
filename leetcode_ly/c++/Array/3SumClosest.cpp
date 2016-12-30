class Solution {
public:
    int threeSumClosest(vector<int>& nums, int target) {
        if(nums.size() < 3){
            return target;
        }
        else{
            vector<int> b = sort(nums);
            int len = b.size();
            int diff = b[0] + b[1] +b[2] - target;
            vector<int> result;
            result.push_back(b[0]);
            result.push_back(b[1]);
            result.push_back(b[2]);
            int currentValue = b[0];
            for(int i = 0; i < len - 1; i++){
                if(currentValue == b[i] && i){
                    continue;
                }
                else{
                    int begin = i + 1;
                    int end = len - 1;
                    while(begin < end){
                        int sum = b[i] + b[begin] + b[end] - target;
                        if(abs(sum) <= abs(diff) && sum < 0){
                            diff = abs(sum);
                            result[0] = b[i];
                            result[1] = b[begin];
                            result[2] = b[end];
                            begin++;
                        }
                        else if(abs(sum) < abs(diff) && sum > 0){
                            diff = abs(sum);
                            result[0] = b[i];
                            result[1] = b[begin];
                            result[2] = b[end];
                            end--;
                        }
                        else if(sum == 0){
                            return b[i] + b[begin] + b[end];
                        }
                        else if(abs(sum) > abs(diff) && sum < 0){
                            begin++;
                        }
                        else{
                        	end--;
						}
                    }
                   
                }
                currentValue = b[i];
            }
            return result[0] + result[1] + result[2];  
        }
    }
   
    vector<int> sort(vector<int>& nums){
        int len = nums.size();
        for(int i = 0; i < len - 1; i++){
            for(int j = i + 1; j < len; j++){
                if(nums[i] >= nums[j]){
                    int temp = nums[i];
                    nums[i] = nums[j];
                    nums[j] = temp;
                }
            }
        }
        return nums;
    }
};