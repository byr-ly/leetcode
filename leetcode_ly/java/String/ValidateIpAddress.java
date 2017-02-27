public class Solution {
    public String validIPAddress(String IP) {
        if(IP == null || IP.isEmpty()) return "Neither";
        if(IP.contains(".")){
            return isIpv4(IP) ? "IPv4" : "Neither";
        }
        else{
            return isIpv6(IP) ? "IPv6" : "Neither";
        }
    }
    
    public boolean isIpv4(String IP){
    		//1.1.1.1.分隔后长度也是4
        if(IP.charAt(IP.length() - 1) == '.') return false;
        String[] str = IP.split("\\.");
        if(str.length != 4) return false;
        for(int i = 0; i < str.length; i++){
        	if(str[i].isEmpty()) return false;
            char[] c = str[i].toCharArray();
            if(c.length > 3) return false;
            for(int j = 0; j < c.length; j++){
                if(c[j] < '0' || c[j] > '9') return false;
            }
            int val = Integer.parseInt(str[i]);
            if(c[0] == '0' && c.length != 1) return false;
            if(val < 0 || val > 255) return false;
        }
        return true;
    }
    
    public boolean isIpv6(String IP){
        if(IP.charAt(IP.length() - 1) == ':') return false;
        String[] str = IP.split(":");
        if(str.length != 8) return false;
        for(int i = 0; i < str.length; i++){
            if(str[i].isEmpty()) return false;
            char[] c = str[i].toCharArray();
            if(c.length > 4) return false;
            for(int j = 0; j < c.length; j++){
                if(!((c[j] >= '0' && c[j] <= '9') || (c[j] >= 'a' && c[j] <= 'f') || (c[j] >= 'A' && c[j] <= 'F'))) return false;
            }
        }
        return true;
    }
}