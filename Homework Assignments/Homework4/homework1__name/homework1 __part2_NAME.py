from mrjob.job import MRJob
from mrjob.step import MRStep
import re
#python shiyan2.py input.txt -o ./output

#python shiyan2.py -r inline input.txt > output1.txt
#python shiyan2.py --reducer_find_max_word


WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_max_word)
        ]

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

    def reducer_find_max_word(self, word, word_count_pairs):
        # none key in this function because in reducer_count_words we discard the key
        # each item of word_count_pairs is (count, word), yield one result: the value(word) of max count
        yield (max(word_count_pairs),word)


# never forget
if __name__ == '__main__':
    MRMostUsedWord.run()
