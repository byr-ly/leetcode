public class Solution {
    public boolean validUtf8(int[] data) {
        if(data == null || data.length == 0) return false;
        for(int i = 0; i < data.length; i++){
            int numOfBytes = 0;
            if(data[i] > 255) return false;
            else if((data[i] & 128) == 0) {//0xxxxxxx
                numOfBytes = 1;
            }
            else if((data[i] & 224) == 192){//110xxxxx 
                numOfBytes = 2;
            }
            else if((data[i] & 240) == 224){//1110xxxx 
                numOfBytes = 3;
            }
            else if((data[i] & 248) == 240){//11110xxx 
                numOfBytes = 4;
            }
            else return false;
            
            for(int j = 1; j < numOfBytes; j++){
                if(i + j >= data.length) return false;
                if((data[i + j] & 192) != 128) return false;
            }
            //-1是因为上面的循环要+1
            i = i + numOfBytes - 1;
        }
        return true;
    }
}