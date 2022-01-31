import subprocess
import os

def run_combiner(parser):

	print("Running combiner...")

	sample_name_map = os.path.abspath(parser.sample_name_map)
	hail_home = '/home/eriurn/submit/python37_intel/lib/python3.7/site-packages/hail/' 
	output_dir = os.path.abspath(parser.output_dir)

	temp_bucket = output_dir
	if parser.tmp_dir:
		temp_bucket = os.path.abspath(parser.tmp_dir)

	spark_submit_script = os.path.join(temp_bucket, "spark_submit_script.py")

	with open(spark_submit_script, "w") as f:

		f.write("import hail as hl\n")
		f.write("hl.init()\n")
		f.write(f"temp_bucket='file://{temp_bucket}'\n")
		f.write(f"output_file='file://{output_dir}'\n")
		f.write(f"path_to_input_list='file://{sample_name_map}'\n")
		f.write("inputs = []\n")
		f.write("with hl.hadoop_open(path_to_input_list, 'r') as f:\n")
		f.write("    for line in f:\n") 
		f.write("        inputs.append(line.strip())\n")

		f.write(f"hl.experimental.run_combiner(inputs, output_file={output_file}, tmp_path={temp_bucket}, reference_genome='GRCh38', use_genome_default_intervals=True, overwrite=True)\n")

	subprocess.run(f"spark-submit --archives ~/submit/pyspark_venv.tar.gz#python37_intel --jars {hail_home}/backend/hail-all-spark.jar {spark_submit_script}",shell=True)
	print("Completed running combiner...")

