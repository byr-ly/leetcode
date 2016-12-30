class Solution {
public:
    string countAndSay(int n) {
        if(n == 1) return "1";
        string front = countAndSay(n - 1);
        int i = 0;
        int j = 0;
        string result = "";
        int count = 0;
        while(j < front.size()){
        	while(front[i] == front[j]){
        		count++;
        		j++;
			}
			char cnt = count + '0';
			result += cnt;
			result += front[i];
			count = 0;
			i = j;
		}
        return result;
    }
};