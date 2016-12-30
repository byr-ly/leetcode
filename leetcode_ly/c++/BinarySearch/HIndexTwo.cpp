class Solution {
public:
    int hIndex(vector<int>& citations) {
        int n = citations.size();
        if(n == 0) return 0;
        int left = 0;
        int right = n - 1;
        while(left <= right){
            int middle = left + (right - left) / 2;
            if(citations[middle] == n - middle) return n - middle;
            else if(citations[middle] > n - middle) right = middle - 1;
            else left = middle + 1;
        }
        return n - left;
    }
};