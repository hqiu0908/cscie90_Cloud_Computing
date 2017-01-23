from collections import Counter
import re

def compareItems((w1,c1), (w2,c2)):
    if c1 != c2:
        return c2 - c1
    else:
        return cmp(w1, w2)
        
filename = '/Users/hqiu/Documents/workspace/WordCounter/src/input.txt'

words = re.findall(r'\w+', open(filename).read().lower())

digits = []
for word in words:
	if word.isdigit():
		digits.append(word)

for digit in digits:
	words.remove(digit)		
	
counts = Counter(words)

items = counts.items()
items.sort(compareItems)
    
print(items)

