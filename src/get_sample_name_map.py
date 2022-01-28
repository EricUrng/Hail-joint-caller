import os

def get_sample_name_map(args):

	# Get directory containing ONLY sample files
	cohort_dir = os.path.abspath(args.cohort_dir)

	# Check output filename
	output_filename = "sample_name_map"
	if args.output:
		output_filename = args.output

	# Check for appending
	writing_format = "w"
	if args.append:
		writing_format = "a"

	# Check for file location
	prepend = "file://"
	if args.hdfs == True:
		prepend = "hdfs://"

	# Write samples to output file
	with open(output_filename, writing_format) as f:
		# Get a list of all the files in sample_dir
		samples = os.listdir(cohort_dir)

		# Narrow it down to only tar.gz files
		for sample in samples:
			if ".tbi" not in sample and ".g.vcf.gz" in sample:
				f.write(prepend + os.path.join(cohort_dir, sample) + "\n")
	# count = 0
	# for filename in filenames:
	# 	if ".tbi" not in filename and ".g.vcf.gz" in filename:
	# 		f.write("file://" + os.path.join(sample_dir,filename) + "\n")
			
	# 		count += 1		
	# 		if count >= num_samples: # Break out when reaching num_samples
	# 			break
		
