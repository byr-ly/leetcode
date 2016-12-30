public class Solution {
    public String getHint(String secret, String guess) {
        int bulls = 0;
        int cows = 0;
        int[] numbers = new int[10];
        for(int i = 0; i < secret.length(); i++){
            if(secret.charAt(i) == guess.charAt(i)) bulls++;
            numbers[secret.charAt(i) - '0']++;
        }
        for(int i = 0; i < guess.length(); i++){
            if(numbers[guess.charAt(i) - '0'] > 0) cows++;
            numbers[guess.charAt(i) - '0']--;
        }
        cows = cows - bulls;
        return bulls + "A" + cows + "B";
    }
}