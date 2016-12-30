class Solution {
public:
    int countPrimes(int n) {
        if(n <= 2){
            return 0;
        }
        bool* isPrimes = new bool[n];
        for(int i = 0; i < n; i++){
            isPrimes[i] = true;
        }
        
        for(int i = 3; i * i < n; i += 2){
            if(isPrimes[i]){
                for(int j = i * i; j < n; j = j + 2 * i){
                    isPrimes[j] = false;
                }
            }
        }
        int count = 1;
        for(int k = 3; k < n; k += 2){
            if(isPrimes[k]){
                count++;
            }
        }
        return count;
    }
};