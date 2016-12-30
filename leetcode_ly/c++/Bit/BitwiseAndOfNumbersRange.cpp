class Solution {
public:
    int rangeBitwiseAnd(int m, int n) {
        int num = INT_MAX;
        while((m & num) != (n & num)){
            num = num << 1;
        }
        return (m & num);
    }
};