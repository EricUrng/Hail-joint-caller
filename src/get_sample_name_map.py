import os

# Get directory containing ONLY sample files
sample_dir = input("Directory containing sample files: ")
sample_dir = os.path.abspath(sample_dir)

# Get number of samples to include
num_samples = int(input("Number of samples to include: "))

f = open("sample_name_map", "w")

# Get a list of all the files in sample_dir
filenames = os.listdir(sample_dir)

# Narrow it down to only tar.gz files
count = 0
for filename in filenames:
	if ".tbi" not in filename and ".g.vcf.gz" in filename:
		f.write("file://" + os.path.join(sample_dir,filename) + "\n")
		
		count += 1		
		if count >= num_samples: # Break out when reaching num_samples
			break
		
