// Forward declaration of isBadVersion API.
bool isBadVersion(int version);

class Solution {
public:
    int firstBadVersion(int n) {
        int low = 1;
        int high = n;
        while(low < high){
            int middle = (high - low) / 2 + low;
            if(isBadVersion(middle)) high = middle;
            else low = middle + 1;
        }
        return high;
    }
};