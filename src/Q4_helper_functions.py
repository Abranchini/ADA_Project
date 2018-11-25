import pandas as pd

def get_emotion_dictionary(data_dir, GCAM_txt):
	""" Get dictionary for emotions
		Input:
		    GCAM-MASTER-CODEBOOK text file
		Output:
		    dataframe of the dictionary
	"""	
	# read data extrated from the GCAM Master Codebook
	data = open(data_dir + GCAM_txt,'r')

	# read it into a list so we can then work with pandas
	Temporary_list = []
	for line in data:
		splitted_line = line.split('\t')
		Temporary_list.append(splitted_line)

	# transform it into a pandas dataframe where the first line is the column name
	df_GCAM_CODEBOOK = pd.DataFrame(Temporary_list[1:], columns=Temporary_list[0])

	# know we want a final dictionary so we can correspond a variable to a emotion or feeling in the event
	Emotions_dictionary = df_GCAM_CODEBOOK[['Variable','DimensionHumanName']]

	return Emotions_dictionary