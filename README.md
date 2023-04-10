# Introduction
This repository contains various artifacts, such as source code, experimental results, and other materials, that supplement our work on mixed covers.\
&nbsp;&nbsp;&nbsp;&nbsp;The code for all experiments in our paper can be found in <kbd>exp/</kbd>, in particular experiment 0, experiment 1, ..., experiment 6.\
In the following sections, we describe how our experiments can be reproduced. 
# Preliminaries: Getting databases ready for experiments
> 1. Import datasets as SQL databases
>> We have used MySQL 8.0 as the underlying database workbench. Firstly, please create a database called "freeman". Afterwards, import the [datasets](https://drive.google.com/drive/folders/1RIO8hRNNwvTn0DU5tuazYWpsaIEtr7ch?usp=sharing) as MySQL databases by setting column names to 0,1,...,n-1 where n is the number of columns in a given dataset. In addition, please create a column named "id" as an auto_increment attribute for each table.
>2. Functional dependencies (FDs)
>> For each of the datasets, we compute all FD covers and their mixed variants. All FD covers/mixed covers can be found as separate json files in <kbd>Artifact/FD/</kbd>.
>3. JDK & JDBC
>> Our code was developed in JAVA. Accordingly, please specify a JDK with version 8 or later. At the moment, we are using JDBC (version 8.0.26) as a connector to MySQL databases.
# Experiments
Our experiments are organized into seven sections. For each of them, you can run different code/scripts:
> 0. Computing FD covers
>> In this experiment, we compute several FD covers with given FDs as input. These FD covers include non-redundant, reduced, canonical, minimal and minimal-reduced covers. In addition, we keep some statistics for each of these FD covers. You can run the code in <kbd>exp/exp0</kbd> on any mainstream IDE like eclipse or IDEA.
> 1. Impact of FD covers on computing minimal keys
>> In this experiment, we determine the time required for computing the set of minimal keys given different FD covers. The state-of-the-art algorithm to compute the set of minimal keys is worst-case exponential, as proposed by Osborne. You can run the code in <kbd>exp/exp1</kbd>.
> 2. Computing mixed covers
>> This experiment computes the mixed variants of given FD covers, such as original, non-redundant, reduced, ... In addition, we record several statistics. The source code for this experiment can be found in <kbd>exp/exp2</kbd>.
> 3. Further refinements of mixed covers
>> In continuation of experiment 2, in experiment 3, we compute the corresponding FD cover for the set of FDs that are not implied the set of minimal keys. For example, after we get a mixed cover of a minimal FD cover, we get the set of minimal keys \Sigma_k and the set of remaining FDs \Sigma_FD, and then we only compute the minimal cover of \Sigma_FD. In addition, we also record some statistics such as size, attribute symbol number and cost. The code in <kbd>exp/exp3</kbd> can be used to run the experiment.
> 4. Performance tests under updates on non-normalized schemata
>> In this experiment, we investigate the update performance for different FD covers on schemata that have not been normalized. To reproduce the experiments, the code in <kbd>exp/exp4</kbd> can be run.
> 5. Performance tests under updates on normalized sub-schemata
>> We study the update performance of different FD covers and mixed covers on sub-schemata resulting from lossless, dependency-preserving decompositions into Third Normal Form (3NF). Note that we limit the number of sub-schemata to at most 10. The code for running this experiment can be found in <kbd>exp/exp5</kbd>.
> 6. Update performance of minimal-reduced and optimal covers on synthesized data sets
>> In this experiment, we create perfect synthetic datasets of varying size by creating disjoint copies of Armstrong relations for our examples on MAIL, TRAFFIC and the generic p-schemata (for p=1,3,5,7,11) from the proofs in our paper. The experiments illustrate the differences in size and update performance between minimal-reduced and optimal FD covers, and between minimal-reduced mixed covers and optimal mixed covers, respectively. For reproduction purposes, please use the code in <kbd>exp/exp6</kbd>.
