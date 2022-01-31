import subprocess
import os

def run_combiner():
	print("Running combiner...")
	subprocess.run("spark-submit --archives ~/pyspark_venv.tar.gz#python37_intel --master yarn --deploy-mode cluster test_script.py",shell=True,capture_output=True)
	print("Completed running combiner...")
	pass

	# Write to a new file called spark_submit_run_combiner.py
	# and then write the same lines from the jupyter notebook to this file
	# taking in the users command line arguments
