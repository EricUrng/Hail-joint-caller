import subprocess
import os

def run_combiner(parser):
	print("Running combiner...")

	with open("spark_submit_script.py", "w") as spark_submit_script:
		spark_submit_script.write("import hail as hl")
		spark_submit_script.write("hl.init()")
		spark_submit_script.write(f"output_file={parser.output_dir}\n")
		if parser.tmp_dir:
			spark_submit_script.write(f"temp_bucket={parser.tmp_dir}\n")
		else:
			spark_submit_script.write(f"temp_bucket={parser.output_dir}\n")

		spark_submit_script.write(f"path_to_input_list={parser.sample_name_map}\n")
		spark_submit_script.write("inputs = []\n")
		spark_submit_script.write("with hl.hadoop_open(path_to_input_list), 'r' as f:\n")
		spark_submit_script.write("    for line in f:\n") 
		spark_submit_script.write("        inputs.append(line.strip())\n")

		spark_submit_script.write("""hl.experimental.run_combiner(inputs,
																  output_file=output_file,
																  tmp_path=temp_bucket,
																  reference_genome='GRCh38',
																  use_genome_default_intervals=True,
																  overwrite=True""")

	print("Completed running combiner...")

	# Write to a new file called spark_submit_run_combiner.py
	# and then write the same lines from the jupyter notebook to this file
	# taking in the users command line arguments
