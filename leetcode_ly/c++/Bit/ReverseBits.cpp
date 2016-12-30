class Solution {
public:
    uint32_t reverseBits(uint32_t n) {
        unsigned int ans = 0;
        unsigned int i;
        for(i = 1; i != 0; i = i << 1){
            ans <<= 1;
            if(n & 1) ans |= 1;
            n >>= 1;
        }
        return ans;
    }
};