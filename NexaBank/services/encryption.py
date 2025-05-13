"""
encryption_method.py

This module provides a simple Caesar cipher-based encryption and decryption utility.
It includes both direct encryption with a known key and brute-force decryption using
English word scoring to determine the most likely key.

Requires:
- `data/english_words.txt`: A space-separated list of English words for scoring.

Features:
- Alphabetic character shifting with case sensitivity
- Punctuation and whitespace preserved
- Brute-force decryption using English word match score
"""

class EncryptionMethod:
    """
    Implements Caesar cipher encryption and brute-force decryption using English dictionary scoring.
    """

    with open("data/english_words.txt") as fp:
        _ENGLISH_WORDS = set(fp.read().split(" "))

    def _shift_alpha(self, msg: str, key: int) -> str:
        """
        Shift alphabetic characters in a message by a given key using Caesar cipher logic.

        Args:
            msg (str): The message to be transformed.
            key (int): Number of positions to shift (positive for encryption, negative for decryption).

        Returns:
            str: Transformed message with shifted characters.
        """
        def _map_char(char: str) -> str:
            if not char.isalpha():
                return char
            start = ord("a") if char.islower() else ord("A")
            return chr(((ord(char) - start + key) % 26) + start)

        return "".join(_map_char(i) for i in msg)

    def _decrypt(self, msg: str, key: int) -> str:
        """
        Internal method to decrypt a message using a specific key.

        Args:
            msg (str): Encrypted message.
            key (int): Caesar cipher key to reverse the encryption.

        Returns:
            str: Decrypted message.
        """
        return self._shift_alpha(msg, -key)

    def encrypt(self, msg: str, key: int) -> str:
        """
        Encrypt a message using Caesar cipher with a given key.

        Args:
            msg (str): Plain text message.
            key (int): Shift value.

        Returns:
            str: Encrypted message.
        """
        return self._shift_alpha(msg, key)

    def decrypt(self, msg: str) -> str:
        """
        Attempt to decrypt a Caesar cipher-encrypted message without knowing the key.
        Uses brute-force and English word scoring to determine the best candidate.

        Args:
            msg (str): Encrypted message.

        Returns:
            str: Best-guess decrypted message.
        """
        def is_english_word(word: str) -> bool:
            return word.lower() in self._ENGLISH_WORDS

        max_score, key = 0, 0

        for i in range(1, 26):
            text = self._decrypt(msg, i)
            score = sum(is_english_word(w) for w in text.split(" "))
            if score > max_score:
                max_score = score
                key = i

        return self._decrypt(msg, key)


if __name__ == "__main__":
    # Example usage
    cipher = EncryptionMethod()
    encrypted = cipher.encrypt("Hello World", 30)
    print(cipher.decrypt(encrypted))