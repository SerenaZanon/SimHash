from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, size, explode
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover
from itertools import combinations, product
import numpy as np
import time
import csv


# Function to retrieve datasets
def retrieve_datasets(sp):
    # Read the parquet file
    df = spark.read.parquet("/home/claudio.lucchese/datasets/enron/train-00000-of-00004.parquet")
    # Create different sizes of datasets
    df_small = df.limit(5000)
    df_medium = df.limit(10000)
    df_big = df.limit(15000)
    return [df_big, df_medium, df_small], ["big", "medium", "small"]


# Function to create combinations of signature bits and blocks
def creating_combinations(signature_bits, lengths):
    return list(product(signature_bits, lengths))


# Function to prepare the data
def prepare_data(df):
    # Removing punctuation
    df = df.select(regexp_replace("text", "[^0-9a-zA-Z_\-]+", " ").alias("text"))

    # Tokenize text
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    words_df = remover.transform(tokenizer.transform(df))

    # Count unique tokens
    unique_tokens = words_df.select(explode(col("filtered_words"))).distinct().count()
    print("\n\nUnique tokens: ", unique_tokens, "\n\n")

    # Compute term frequencies
    hashingTF = HashingTF(numFeatures=unique_tokens, inputCol="filtered_words", outputCol="rawFeatures")
    featured_data = hashingTF.transform(words_df)

    # Calculate Inverse Document Frequencies (IDF)
    idf = IDF(inputCol="rawFeatures", outputCol="TFIDF")
    idf_model = idf.fit(featured_data)
    tfidf_data = idf_model.transform(featured_data).select("TFIDF").rdd.flatMap(lambda x: x).zipWithIndex().map(
        lambda x: (x[1], x[0]))
    return tfidf_data, unique_tokens


# Function to create the random matrix (1, -1)
def create_random_matrix(signature_bits, n_terms):
    return np.random.choice([-1, 1], size=(signature_bits, n_terms))


# Function to split the signature into blocks
# It creates a list composed by tuples (position, block, signature_id, signature)
# The block is transformed in an integer
def split_signature(signature, block_len):
    results = []
    for i in range(0, len(signature[1]), block_len):
        position = i // block_len
        block = signature[1][i:i + block_len]
        str_block = ''.join(map(str, block))
        int_block = int(str_block, 2)
        results.append((position, int_block, signature[0], signature[1]))
    return results


# Function to save data to CSV
def to_csv(data, file_name):
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        for info in data:
            writer.writerow(info)


# Create a SparkSession
spark = (SparkSession.builder.appName("Assignment 2")
         .getOrCreate())
sc = spark.sparkContext

# Specifying some data
m = [64, 128, 256]
length_blocks = [4, 8, 16, 32]
threshold = 0.95

# Retrieve datasets
datasets, datasets_name = retrieve_datasets(spark)

# Create combinations of signature bits and blocks
possible_combinations = creating_combinations(m, length_blocks)

# Iterate over datasets and their names
for dataset, dataset_name in zip(datasets, datasets_name):

    dataset_info = []

    tfidf_rdd, num_tokens = prepare_data(dataset)
    print("\n\nPreparing dataset done!\n\n")

    for values in possible_combinations:

        print(f"\nName dataset: {dataset_name}\n\n")
        print(f"\n\nNumber of rows: {dataset.count()}\n\n")
        print(f"\n\n\nUsing {values[0]} number of bits\n\n\n")
        print(f"\n\nLength of the blocks: {values[1]} \n\n")

        init_tot_exe = time.time()
        # Create a random matrix and broadcast it
        matrix_rand = sc.broadcast(create_random_matrix(values[0], num_tokens))



        # Function to compute signature
        def compute_signature(tf_idf):
            result = np.zeros(matrix_rand.value.shape[0])
            for idx, value in zip(tf_idf.indices, tf_idf.values):
                result += value * matrix_rand.value[:, idx]
            signature = (result >= 0).astype(int).tolist()
            return signature


        # Compute signatures for documents
        doc_signatures = tfidf_rdd.mapValues(lambda doc: compute_signature(doc)).cache()

        print("\n\nComputing signatures done!\n\n")

        # Split signatures into blocks
        block_signatures = doc_signatures.flatMap(lambda row: split_signature(row, values[1])).cache()

        print("\n\nSplitting signatures done!\n\n")

        # Group signatures by block position and block content
        group_signatures = (block_signatures.map(lambda x: ((x[0], x[1]), x[2]))
                            .groupByKey().mapValues(list))

        print("\n\nGrouping by posBlock and block done!\n\n")

        # Create pairs of signatures within the same group
        pairs_signatures = group_signatures.flatMap(lambda block: list(combinations(block[1], 2))).distinct()

        print("\n\nCreating and collecting unique pairs done!\n\n")


        # Function to compute similarity between two signatures
        def compute_similarity(doc1, doc2):
            signature1 = doc_signatures_map.value.get(doc1)
            signature2 = doc_signatures_map.value.get(doc2)
            difference = np.sum(np.array(signature1) != np.array(signature2))
            simhash = 1 - (difference / len(signature1))
            return doc1, doc2, float(simhash)

        # Compute similarity for pairs
        doc_signatures_map = sc.broadcast(doc_signatures.collectAsMap())
        sim_signatures = pairs_signatures.map(lambda x: compute_similarity(x[0], x[1]))

        print("\n\nComputing similarity done!\n\n")

        # Filter similar documents based on threshold
        pairs_similar_documents = sim_signatures.filter(lambda row: row[2] >= threshold).distinct().count()

        print("\n\nFinding pairs of similar documents done!\n\n")
        total_exe_time = time.time() - init_tot_exe
        # Calculate total execution time
        dataset_info.append([values[0], values[1], pairs_similar_documents, total_exe_time])

    # Save dataset info to CSV
    to_csv(dataset_info, dataset_name + "_full_resources" + ".csv")


