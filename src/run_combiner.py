import subprocess
import os

def run_combiner():
	print("Running combiner...")
	subprocess.run("spark-submit --archives ~/pyspark_venv.tar.gz#python37_intel --master yarn --deploy-mode cluster test_script.py",shell=True,capture_output=True)
	print("Completed running combiner...")
	pass
