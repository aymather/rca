import fuzzyset
import numpy as np

# Class to abstract away a lot of the fuzzyset steps
# For fast cosine similarity
class Fuzz:
    
    def __init__(self, strings):
        
        # Detach reference
        strings = np.array(strings)
        
        fuzzyset, dictionary = self.create_fuzzyset(strings)
        
        self.fuzzyset = fuzzyset
        self.dictionary = dictionary
        
    def check(self, string):
        
        string = self.cos_preprocess(string)
        
        if len(string) == 0:
            return 0, '', False
        
        res = self.fuzzyset.get(string)
        
        # If we got absolutely nothing, return nothing
        if res is None:
            return 0, '', False
        
        # Extract the ratio and the string it matches
        ratio, match = res[0]
        
        # Now we want to add additional points if there are any substring matches
        add, isMatch = self.add_substring_points(string, match, ratio)
        ratio += add
        
        m = self.dictionary[match] if match in self.dictionary else None
        
        return ratio, m, isMatch
    
    # Return the number of points we should add for substring matches
    def add_substring_points(self, string, match, ratio):
        
        """
        Strategy:
        
        We are searching for exact matches between our string, and the
        string it matches to. First we get the residual between the returned
        cosine similarity ratio, and 1. Those are essentially "the number of points
        taxed". Then we break up the string into substrings separated by spaces.
        Those substrings are then searched in the match string for an exact match.
        If an exact match is found, then we divide the residual by 2 and add that
        to the additional points. Basically the point is to really inflate a low
        scoring number, that also has an exact match, because more often than not,
        having an exact match means we found something. Unless the string is really
        way off.
        """
        
        # Prep
        residual = 1 - ratio
        additional = 0
        isMatch = False
        
        # Split up the string
        substrings = string.split()
        
        # Loop through substrings in original string
        for substring in substrings:
            
            # If there is an exact match, add points (maximum of half the residual)
            if substring in match:
                
                # Coefficient value is exponentially increased by the number of
                # letters that the matching substring contains.
                # In other words, longer substring matches are rewarded more than
                # shorter substring matches.
                c = (np.exp(len(substring) / 10)) - 1
                
                # Apply coefficient to residual, value cannot be more than half
                # the remaining residual.
                value = np.min([residual * c, residual / 2])
                
                # Apply
                additional += value
                residual -= value
                isMatch = True
                
        return additional, isMatch
        
    # Create a preprocessed fuzzyset to compare against
    def create_fuzzyset(self, strings):

        # Init
        fuzz = fuzzyset.FuzzySet()
        dictionary = {}

        # Preprocess the strings in the set
        for i in range(len(strings)):

            # Preprocess the string for cosine similarity
            pstring = self.cos_preprocess(strings[i])
            
            # Save a reference to the original string
            dictionary[pstring] = strings[i]
            
            # Modify into the preprocessed string
            strings[i] = pstring

        # Add to the fuzzyset if there is anything left
        for s in strings:
            if len(s) > 0:
                fuzz.add(s)

        return fuzz, dictionary

    # Preprocess a string for cosine similarity
    def cos_preprocess(self, string):

        if string is None:
            string = ''

        # Remove all special characters
        string = ''.join(s for s in string if s.isalnum())

        # Lowercase string
        string = string.lower()

        # Alphabetize
        string = string.split()
        string.sort()
        string = ' '.join(string)

        # Trim final whitespace
        string = ''.join(string)

        return string