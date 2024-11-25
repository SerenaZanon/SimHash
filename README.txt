The project is characterized by diferrent files:

PYTHON FILE
- pair_similarity.py contains the whole implementation of similarity calculation between documents. To create different files based on the number of adopted cores the name of the outcoming csv file was changed manually.
- count_pairs.py contains the code that permits to count the number of duplicated and distinct pairs on which the similarity is computed.

JUPYTER NOTEBOOK FILE
- analysis.ipynb and analysis_pt2.ipynb contains the instructions to read the csv files created by the py files in order to visualize the execution time trends, in addition to bar graphs representing the number of similar pairs found (These are not included in the report for space limitations).

results FOLDER
This folder contains:
- csv files obtained by running the py files;

The project can be runned with the following commands:
	!!! As stated in the report, the results of the project were obtained by running it on the DAIS cluster !!!
1) spark-submit --executor-cores 4 --executor-memory 14g --master spark://didavhpc04:7077 pair_similarity.py
	To use all available cores (12)

2) spark-submit --total-executor-cores 8 --executor-memory 14g --master spark://didavhpc04:7077 pair_similarity.py
	To use 8 cores

1) spark-submit --total-executor-cores 4 --executor-memory 14g --master spark://didavhpc04:7077 pair_similarity.py
	To use 4 cores