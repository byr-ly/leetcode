class Solution {
public:
    bool isPalindrome(int x) {
        if(x < 0){
            return false;
        }
        int fast = x;
        int slow = x;
        int result = 0;
        while(fast != 0){
            result = result * 10 + slow % 10;
            slow /= 10;
            fast /= 100;
        }
        return result == slow || slow == result / 10;
    }
};