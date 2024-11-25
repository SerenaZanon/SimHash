# University course: LEARNING WITH MASSIVE DATA
# Assignment: Approximate Near-Duplicate Detection with SimHash

The SimHash signature consists in the concatenation of m single-bit signatures.

The SimHash signature can be used to estimate the cosine similarity of two documents.

**Computing SimHash (Option 1)**
- Pick a random vector $r_w$ = $\text{{}+1,-1\text{}}^{|d|}$ made of +1,-1 drawn uniformly at random (as many as the length |d| of document vectors)
- Given a document A, compute the dot product (r x A) (○ same as checking on which side of the hyperplane A is)
- If this is positive then the single bit signature is 1 otherwise it is 0
- Repeat m times to compute a SimHash signature of m bits.

**Computing SimHash (Option 2)**
- For each word w pick a random vector $r_w$ = $\text{{}+1,-1\text{}}^m$ of length m made of +1,-1
- Initialize the signature to 0^m
- For each word w in document A, add to the signature the corresponding vector to rw multiplied by the weight of w (weight can be 1, or tf-idf, etc…)
- The sign of the final signature is the SimHash of the document

**Speeding up SimHash**
- To avoid quadratic complexity, a signature is split in small pieces
- Given a collection of documents and their SimHash:
  - first identify groups of documents sharing at least one piece (in the same position)
  - then compute actual hash similarity of all possible pairs in each group (potentially one might compute also actual similarity to filter out false positives)

**SimHash Wrap-up**
- Input:
  - simhash size m,
  - number of pieces p,
  - minimum similarity s (cosine or simhash overlap)
  - collection of documents D
- Process your corpus and transform it into a (documents x words) matrix
  - words or any other feature
- Compute the Simhash of each document and split it into pieces
- Find groups of documents that share at least one piece
- Inside each groups compare the SimHash of every pair of documents to find the similar ones
- Anything else you find useful to validate the method, remove false positives, speedup the processing, avoid redundant computations
