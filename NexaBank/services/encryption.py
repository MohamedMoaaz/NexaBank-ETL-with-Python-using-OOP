class EncryptionMethod:
    with open("data/english_words.txt") as fp:
        _ENGLISH_WORDS = set(fp.read().split(" "))

    def _shift_alpha(self, msg: str, key: int) -> str:
        def _map_char(char: str) -> str:
            if not char.isalpha():
                return char

            start = ord("a") if char.islower() else ord("A")
            return chr(((ord(char) - start + key) % 26) + start)

        return "".join(_map_char(i) for i in msg)

    def _decrypt(self, msg: str, key: int) -> str:
        return self._shift_alpha(msg, -key)

    def encrypt(self, msg: str, key: int) -> str:
        return self._shift_alpha(msg, key)

    def decrypt(self, msg: str) -> str:
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
    cipher = EncryptionMethod()

    encrypted = cipher.encrypt("Hello World", 20)
    print(cipher.decrypt(encrypted))
