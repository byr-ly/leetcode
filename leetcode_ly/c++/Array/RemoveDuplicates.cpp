ndfsuhfisdhfusclass Solution {
public:
    int removeDuplicates(vector<int>& nums) {
        if(nums.size() == 0){
		    return 0;
	    }
	    else{
		    int len = nums.size();
	        int count = 0;
	        vector<int>::iterator it = nums.begin();
	        for(int i = 1; i < nums.size(); i++){
		        if(nums[i] == nums[i-1]){
		        	nums.erase(it);
		        	i--;
		        	count++;
	        	}
	        	else{
	        		it++;
	        	}
        	}
        	return (len-count);
        	}
    }
};
