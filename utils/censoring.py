class CensorMessages:

    def censor_text(self, text: str, words: list[str]) -> str:
        """
        Receives text to be censored
        List of banned words
        Replaces banned words in the text with placeholder
        """

        if not text:
            return text
        
        self.censored_text = text

        for word in words:
            self.censored_text = self.censored_text.replace(word, "*censored*")
        
        return self.censored_text