// Forward declaration of guess API.
// @param num, your guess
// @return -1 if my number is lower, 1 if my number is higher, otherwise return 0
int guess(int num);

class Solution {
public:
    int guessNumber(int n) {
        int low = 1;
        while(low <= n){
            int middle = low + (n - low) / 2;
            if(guess(middle) == 1) low = middle + 1;
            else if(guess(middle) == -1) n = middle - 1;
            else return middle;
        }
        return -1;
    }
};