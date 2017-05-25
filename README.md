## Synopsis
Author Detection System that provides a ranked list of possible authors for a given text file. The two parts to the project are an offline analysis, and online analysis. The offline portion takes input as a set of books and creates unigram analysis using TFIDF vectors for each author. The online portion then creates a TFIDF vector for the unknown author and calculates the cosine similarity across all authors in the the offline analysis.

## Motivation

This project was written as a programming assignment for CS435 Introduction to Big Data at Colorado State University. The project was designed to be an introduction to MapReduce and Hadoop. The project requirements are in the main repo in the instructions file.

## Calculations

The project uses two main calculations for its final result. The offline portion creates author attribute vectors containing the TFIDF values for all of the possible words across every author. The online portion calculates the cosine similarity for the unknown author across all of the other authors to find the most likely writer. The formulas used are in the instructions file.

## Tests

The Author Cosine similarity test is used for testing the AuthorCosSim class. JUnit is being used for the test.

## Contributors

Daniel Lund (daniellund14)
