class Solution {
public:
    void nextPermutation(vector<int>& nums) {
        int len = nums.size();
        int i;
        // 1.�Ӻ���ǰ���ҵ���һ�� A[i-1] < A[i]�ġ�2,4,6,5,3,1 ���Կ���A[i]��A[n-1]��Щ���ǵ����ݼ����С�
        for(i = len - 1; i >= 0; i--){
            if(nums[i] <= nums[i - 1]) continue;
            else break;
        }
        if(i == 0){
            sort(nums.begin(),nums.end());
            return;
        }
        //2.�� A[n-1]��A[i]���ҵ�һ����A[i-1]���ֵ��Ҳ����˵��A[n-1]��A[i]��ֵ���ҵ���A[i-1]��ļ����е���С��һ��ֵ��
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
        	//3.���� ������ֵ�����Ұ�A[n-1]��A[i+1]���򣬴�С����
        int temp;
        temp = nums[i - 1];
        nums[i - 1] = nums[index];
        nums[index] = temp;
        sort(nums.begin() + i,nums.end());
    }
};