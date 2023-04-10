# Introduction
This repository contains various artifacts, such as source code, experimental results, and other materials, that supplement our work on mixed covers.\
&nbsp;&nbsp;&nbsp;&nbsp;The code for all experiments in our paper can be found in <kbd>exp/</kbd>, in particular experiment 0, experiment 1, ..., experiment 5.\
In the following sections, we describe how our experiments can be reproduced. 
# Preliminaries: Getting databases ready for experiments
> 1. Import datasets as SQL databases
>> We have used MySQL 8.0 as database workbench. Firstly, please create a database called "freeman". Afterwards, import the [datasets](https://drive.google.com/drive/folders/1RIO8hRNNwvTn0DU5tuazYWpsaIEtr7ch?usp=sharing) as MySQL databases by setting column names as 0,1,...,n-1 where n is the number of columns in a given dataset. In addition, please create a column named "id" as an auto_increment attribute for each table.
>2. Functional dependencies (FDs)
>> For each of the datasets, we compute all FD covers and mixed covers. You can find all FD covers/mixed covers given as separate json files in <kbd>Artifact/FD/</kbd>.
>3. JDK & JDBC
>> Our code was developed in JAVA. As a consequence, please specify a JDK with version 8 or later. At the moment, we are using JDBC (version 8.0.26) as a connector to MySQL databases.
# Experiments
Our experiments are organized into seven sections. For each of them, you can run different code/scripts:
> 0. Computing FD covers
>> In this experiment, we compute and save several FD covers with given FDs as input. These FD covers include non-redundant, reduced, canonical, minimal and reduced minimal covers. Then, we statistics some properties when computing these FD covers. Your can run source code in <kbd>exp/exp0</kbd> on mainstream IDEs like eclipse or IDEA.
> 1. Impact of FD covers on computing minimal keys
>> In this experiment, we investigate the influence (time required) of different FD covers as input on computing minimal keys. The algorithm to compute minimal keys is a worst-case exponential time algorithm, proposed by Osborne. You can run source code in <kbd>exp/exp1</kbd>.
> 2. Computing mixed covers
>> This experiment computes mixed covers, with given FD set as input, such as original, non-redundant, reduced, ... Then, the experiment statistics some properties when computing mixed covers. In <kbd>exp/exp2</kbd>, it is the source code for experiment 2.
> 3. Further mixed cover refining
>> Following up experiment 2, in experiment 3, we compute corresponding FD cover of \Sigma_FD of mixed cover of corresponding cover type. For example, after we get a mixed cover of a minimal FD cover, we get minimal keys \Sigma_k and remaining FDs \Sigma_FD, and then we only compute the minimal cover of \Sigma_FD further. Finally we keep some stats like size, attribute symbol number and cost. The code in <kbd>exp/exp3</kbd> shows how to reproduce.
> 4. Performance tests under updates on non-normalized schemas
>> In this experiment, we investigate the performance of different FD covers of non-normalized schemas on update tests. To reproduce the experiments, you just run the code in <kbd>exp/exp4</kbd>.
> 5. Performance tests under updates on normalized subschemas
>> We study subschema performance under updates, which resulted from sub-schemas of lossless decomposition algorithm for each sub-schema (10 at most) in 3NF, we get different FD covers, mixed covers from original FDs. Then we have some subschema variants with different equivalent FD set, for each subschema variant, we do update experiments. You can simply configure some parameters in the main function of code of <kbd>exp/exp5</kbd> and then run it for reproduction.
> 6. Performance tests under updates on synthesized data sets
>> In this experiment, with given schema and FDs, we synthesize Armstrong Relation and its copies in different sizes. First, we conduct experiments on Mail and Traffic schemas. Then, computing the optimal cover and mixed optimal cover of FDs. With this two covers and different sizes of Armstrong Relation, do update experiments. Second, we also do the same experiments on p-schemas that p varies from 1,3,5,7,11. In this experiment, we show the difference between reduced minimal covers and optimal covers. For reproduction, see code in <kbd>exp/exp6</kbd>.
