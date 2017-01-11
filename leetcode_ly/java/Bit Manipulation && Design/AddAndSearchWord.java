class TrieNode{
    TrieNode[] children = new TrieNode[26];
    String val = "";
}

public class WordDictionary {
    TrieNode root = new TrieNode();

    // Adds a word into the data structure.
    public void addWord(String word) {
        TrieNode head = root;
        for(int i = 0; i < word.length(); i++){
            char c = word.charAt(i);
            if(head.children[c - 'a'] == null){
                head.children[c - 'a'] = new TrieNode();
            }
            head = head.children[c - 'a'];
        }
        head.val = word;
    }

    // Returns if the word is in the data structure. A word could
    // contain the dot character '.' to represent any one letter.
    public boolean search(String word) {
        return match(word,0,root);
    }
    
    public boolean match(String word,int k,TrieNode root){
        if(k == word.length()) return !root.val.equals("");
        
        if(word.charAt(k) != '.'){
            int idx = word.charAt(k) - 'a';
            return root.children[idx] != null && match(word,k + 1,root.children[idx]);
        }
        else{
            for(int i = 0; i < 26; i++){
                if(root.children[i] != null && match(word,k + 1,root.children[i])) return true;
            }
            return false;
        }
    }
}

// Your WordDictionary object will be instantiated and called as such:
// WordDictionary wordDictionary = new WordDictionary();
// wordDictionary.addWord("word");
// wordDictionary.search("pattern");