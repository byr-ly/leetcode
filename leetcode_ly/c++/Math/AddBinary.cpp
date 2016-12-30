class Solution {
public:
    string addBinary(string a, string b) {
        int al = a.size();
        int bl = b.size();
        if(al > bl) return addBinary(b,a);
        int cn = 0;
        string p = "";
        for(int i = al - 1; i >= 0; i--){
            int result = (int)((a[i] - '0') + (b[i + bl - al] - '0')) + cn;
            if(result == 2){
                p += '0';
                cn = 1;
            }
            else if(result == 3){
                p += '1';
                cn = 1;
            }
            else if(result == 0){
                p += '0';
                cn = 0;
            }
            else{
                p += '1';
                cn = 0;
            }
        }
        for(int j = bl - al - 1; j >= 0; j--){
            int res = (int)(b[j] - '0') + cn;
            if(res == 2){
                p += '0';
                cn = 1;
            }
            else if(res == 3){
                p += '1';
                cn = 1;
            }
            else if(res == 0){
                p += '0';
                cn = 0;
            }
            else{
                p += '1';
                cn = 0;
            }
        }

        if(cn == 1){
            p += '1';
        }
        int m,n;
        for(m = 0, n = p.size() - 1; m < n; m++, n--){
            char temp = p[m];
            p[m] = p[n];
            p[n] = temp;
        }
        return p;
    }
};