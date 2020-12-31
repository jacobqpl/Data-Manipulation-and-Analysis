import mrjob
from mrjob.job import MRJob
import re

WORD_RE = #regular expression

class SixLetterWordCount(MRJob):
  OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
  
  def mapper(self, _, line):
    words = WORD_RE.findall(line)
    # +++your code here+++

  def combiner(self, word, counts):
    # +++your code here+++
        
  def reducer(self, word, counts):
    # +++your code here+++

if __name__ == '__main__':
  SixLetterWordCount.run()