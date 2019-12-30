# Parallel-Matrix-Multiplication
Sparse and Dense Parallel Matrix Multiplication using Hadoop and AWS EMR

* Our goal for the project is to multiply two huge matrices using MapReduce of which one task is multiplying sparse matrices using H-V partitioning and other is multiplying dense matrices using B-B partitioning.
* We were able to successfully complete the Sparse H-V and Dense B-B matrix multiplication and applied Sparse H-V matrix multiplication to find out pagerank of nodes in any graph.
* For Sparse H-V, we conducted four experiments for speedup, scalability, partitioning and comparison of pagerank for a synthetic graph using the Homework4 MR algorithm and matrix multiplication.
* For Dense B-B, we conducted three experiments for speedup, scalability and partitioning.
* Achieved speedup of 2.1 times with 2x the cluster size and good amount of scalability with controlled partitioning