class Solution {
public:
    int maxRotateFunction(vector<int>& A) {
        if(A.size() <= 1) return 0;
        int sum = 0;
        int num = 0;
        for(int i = 0; i < A.size(); i++){
            sum += A[i];
            num += i * A[i];
        }
        
        int max = INT_MIN;
        for(int i = A.size() - 1; i > 0; i--){
            max = (num > max) ? num : max;
            num = num - (A.size() - 1) * A[i] + (sum - A[i]);
        }
        max = (num > max) ? num : max;
        return max;
    }
};