# University course: LEARNING WITH MASSIVE DATA
# Assignment: Approximate Near-Duplicate Detection with SimHash

SimHash Wrap-up
- Input:
  - simhash size m,
  - number of pieces p,
  - minimum similarity s (cosine or simhash overlap)
  - collection of documents D
- Process your corpus and transform it into a (documents x words) matrix**
  - words or any other feature
- Compute the Simhash of each document and split it into pieces**
- Find groups of documents that share at least one piece
- Inside each groups compare the SimHash of every pair of documents to find the similar ones
- Anything else you find useful to validate the method, remove false positives, speedup the processing, avoid redundant computations
