class TrieNode {
    // Initialize your data structure here.
    TrieNode[] children = new TrieNode[26];
    char val;
    boolean isWord = false;
    
    public TrieNode() {
        val = ' ';
    }
    
    public TrieNode(char c){
        val = c;
    }
}

public class Trie {
    private TrieNode root;

    public Trie() {
        root = new TrieNode();
    }

    // Inserts a word into the trie.
    public void insert(String word) {
        TrieNode head = root;
        for(int i = 0; i < word.length(); i++){
            char c = word.charAt(i);
            if(head.children[c - 'a'] == null){
                head.children[c - 'a'] = new TrieNode(c);
            }
            head = head.children[c - 'a'];
        }
        head.isWord = true;
    }

    // Returns if the word is in the trie.
    public boolean search(String word) {
        TrieNode head = root;
        for(int i = 0; i < word.length(); i++){
            char c = word.charAt(i);
            if(head.children[c - 'a'] == null) return false;
            head = head.children[c - 'a'];
        }
        return head.isWord;
    }

    // Returns if there is any word in the trie
    // that starts with the given prefix.
    public boolean startsWith(String prefix) {
        TrieNode head = root;
        for(int i = 0; i < prefix.length(); i++){
            char c = prefix.charAt(i);
            if(head.children[c - 'a'] == null) return false;
            head = head.children[c - 'a'];
        }
        return true;
    }
}

// Your Trie object will be instantiated and called as such:
// Trie trie = new Trie();
// trie.insert("somestring");
// trie.search("key");