# Hail-joint-caller [Do we need a name for this?]

## Traditional Methods
The code in this repository implements Hail and Apache Spark to solve the issues surrounding the traditional methods of joint-calling. The N+1 problem involves adding another sample to your cohort. Adding even a single sample requires you to re-call all samples from scratch again. This issue is compounded by the exponential scaling of computational time required as cohort size grows. Hail's new sparse matrix data format addresses this issue and enables the appending of existing joint-calls without re-processing, solving the N+1 problem.


## Scripts available
Currently, processes are separated into their individual scripts. Automating the processes can be completed simply using a workflow like Nextflow.

### **combiner_gvcf.py**
<pre><code> A tool for utilising Hail for joint calling

usage: combine_gvcf.py [-h] [--hdfs] [--overwrite] [--app_name app_name]
                       [--reference reference]
                       sample_map output [temp-dir]

positional arguments:
    sample_map            path to the file containing a list of samples
    output                path to the output file
    temp-dir              path for intermediate files

optional arguments:
    -h, --help            show this help message and exit
    --hdfs                sample_name_map points to samples stored on the hdfs
    --overwrite           overwrite existing output file
    --app_name app_name   name of the application
    --reference reference human genome reference to be used</code></pre>

### mt_to_vcf.py

### load_vqsr.py

### make_finalised_mt.py

### QC_methods.py

### VEP.py

## Submitting a Spark Job


## Examples
Further examples and explanations can be found within the notebooks directory.


## Acknowledgements
This project could not have been completed without the guidance of Joe Copty, Shyamsundar Ravishankar and Arash Bayat.