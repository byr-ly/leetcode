class Solution {
public:
    string getHint(string secret, string guess) {
        int bulls = 0;
        int cows = 0;
        vector<int> result(10,0);
        string s = "";
        for(int i = 0; i < secret.size(); i++){
            if(secret[i] == guess[i]){
                bulls++;
            }
            result[secret[i] - '0']++;
        }
        for(int j = 0; j < guess.size(); j++){
            if(result[guess[j] - '0'] > 0){
                cows++;
                result[guess[j] - '0']--;
            }
        }
        cows = cows - bulls;
        s = s.append(to_string(bulls) + "A" + to_string(cows) + "B");
        return s;
    }
};