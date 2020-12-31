from mrjob.job import MRJob

import re


# python homework1 __part1_NAME.py -r inline input.txt > output.txt

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):



    def mapper(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner(self, word, counts):
        # optimization: count the words we have seen so far
        yield (word, sum(counts))

    def reducer(self, word, counts):
        # send all (num_occupences, word) pairs to same reducer
        # use sum(num_occupences) to get the total num of occupences of each word
        yield None,(sum(counts), word)


# never forget
if __name__ == '__main__':
    MRMostUsedWord.run()
