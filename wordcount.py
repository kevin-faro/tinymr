from tinymr import MapRed
from tinymr import Context
import string
import sys

def mapper(lineNumber, line, context):
    for word in line.lower().replace(string.punctuation,'').split(' '):
        context.emit(word, 1)

def reducer(key, values, context):
    sum = 0
    for value in values:
        sum += int(value)
    context.emit(key, sum)

if __name__ == "__main__":
    MapRed.MapRed.run(mapper, reducer)
