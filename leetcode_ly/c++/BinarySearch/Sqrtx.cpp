class Solution {
public:
    int mySqrt(int x) {
        int low = 1;
        int high = x;
        while(low <= high){
            long middle = low + (high - low) / 2;
            if(middle * middle == x) return middle;
            else if(middle * middle > x) high = middle - 1;
            else low = middle + 1;
        }
        return high;
    }
};