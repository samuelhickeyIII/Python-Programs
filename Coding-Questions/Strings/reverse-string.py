"""
Write a function that reverses a string. The input string is given as an array of characters s.

You must do this by modifying the input array in-place with O(1) extra memory.
"""

class Solution:
    def reverseString(self, s: List[str]) -> None:
        """
        Do not return anything, modify s in-place instead.
        """
        # solution #1
        # s = s.reverse() 
        
        # solution #2
        i, j = 0, len(s)-1
        while i < j:
            s[i], s[j] = s[j], s[i]
            i, j = i+1, j-1