class Solution {
public:
    vector<vector<int>> threeSum(vector<int>& nums) {
        vector<vector<int> > list;
        if(nums.size() < 3){
            return list;
        }
        int len = nums.size();
        vector<int> c = sort(nums);
        vector<int> result;
        int currentValue = c[0];
        for(int i = 0; i < len; i++){
            if(i && currentValue == c[i]){
        		continue;
			}
			else{
			    int begin = i + 1;
            	int end = len - 1;
            	while(begin < end){
        		if(c[i] + c[begin] + c[end] == 0){
        			result.push_back(c[i]);
        			result.push_back(c[begin]);
        			result.push_back(c[end]);
        			list.push_back(result);
			        result.clear();
        			begin++;
        			end--;
				}
				else if(c[i] + c[begin] + c[end] < 0){
					begin++;
				}
				else{
					end--;
				}
				currentValue = c[i];
		    	}   
			}
		}
		vector<vector<int> >::iterator it = unique(list.begin(),list.end());      	
		list.resize( distance(list.begin(), it) );
        return list;
    }

    vector<int> sort(vector<int>& nums){
        int len = nums.size();
        	for(int i = 0 ; i < len - 1; i++){
        		for(int j = i + 1 ; j < len; j++){
        			if(nums[i] > nums[j]){
        				int temp = nums[i];
        				nums[i] = nums[j];
        				nums[j] = temp;
        			}
        			else{
        				continue;
        			}
        		}
        	}
    	return nums;
    }
};