import subprocess
import os

def run_combiner(parser):

	print("Running combiner...")

	with open("spark_submit_script.py", "w") as spark_submit_script:

		sample_name_map = os.path.abspath(parser.sample_name_map)
		output_dir = os.path.abspath(parser.output_dir)

		temp_bucket = output_dir
		if parser.tmp_dir:
			temp_bucket = os.path.abspath(parser.tmp_dir)

		spark_submit_script.write("import hail as hl\n")
		spark_submit_script.write("hl.init()\n")
		spark_submit_script.write(f"temp_bucket={temp_bucket})\n")
		spark_submit_script.write(f"output_file={output_dir}\n")
		spark_submit_script.write(f"path_to_input_list={sample_name_map}\n")
		spark_submit_script.write("inputs = []\n")
		spark_submit_script.write("with hl.hadoop_open(path_to_input_list), 'r' as f:\n")
		spark_submit_script.write("    for line in f:\n") 
		spark_submit_script.write("        inputs.append(line.strip())\n")

		spark_submit_script.write("""hl.experimental.run_combiner(inputs, output_file=output_file, tmp_path=temp_bucket, reference_genome='GRCh38', use_genome_default_intervals=True, overwrite=True""")

	print("Completed running combiner...")

