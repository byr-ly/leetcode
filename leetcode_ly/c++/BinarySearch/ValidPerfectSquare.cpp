class Solution {
public:
    bool isPerfectSquare(int num) {
        int low = 1;
        int high = num;
        while(low <= high){
            long middle = low + (high - low) / 2;
            if(middle * middle == num) return true;
            else if(middle * middle > num) high = middle - 1;
            else low = middle + 1;
        }
        return false;
    }
};