import os
import pandas as pd
import matplotlib.pyplot as plt


#################################################### Milestone 3 helper functions

## data processing
def concat_parquets_info(directory,drop_flag,Q4=False,Q5=False):
	""" Get dataframe from parquets
		Input:
		    directory of files
		Output:
		    dataframe with al parquets information
	"""	

	# list of months and years that we read from the cluster GDELT
	Years_Months = ["2015_02","2015_03","2015_04","2015_05","2015_06",
                "2015_07","2015_08","2015_09","2015_10","2015_11","2015_12",
                
                "2016_01","2016_02","2016_03","2016_04","2016_05","2016_06",
                "2016_07","2016_08","2016_09","2016_10","2016_11","2016_12",
                
                "2017_01","2017_02","2017_03","2017_04","2017_05","2017_06",
                "2017_07","2017_08","2017_09","2017_10","2017_11"]

	if Q4 == True:
		df_counter = 0
		for filename in os.listdir(directory):
		    if df_counter == 0:
		        df_0 = pd.read_csv(directory + '/' + filename,sep='\t', encoding='utf-8')
		        df_0 = df_0.drop(['Unnamed: 0'], axis=1)
		        df_0["year"] = Years_Months[df_counter]
		    else:
		        df_1 = pd.read_csv(directory + '/' + filename,sep='\t', encoding='utf-8')
		        df_1 = df_1.drop(['Unnamed: 0'], axis=1)
		        df_1["year"] = Years_Months[df_counter]
		        df_0 = df_0.append(df_1)
		    df_counter += 1


	if Q5 == True:
	    # create counter to know the year and motnh of the parquet
		df_counter = 0
		for filename in os.listdir(directory):
			# initialize dataframe
		    if df_counter == 0:
		        df_0 = pd.read_csv(directory + '/' + filename,sep='\t', encoding='utf-8')
		        df_0 = df_0.drop(['Unnamed: 0'], axis=1)
		        df_0["year"] = filename[0:4]

		    # append the others dataframe to the initialized one before
		    else:
		        df_1 = pd.read_csv(directory + '/' +filename,sep='\t', encoding='utf-8')
		        df_1 = df_1.drop(['Unnamed: 0'], axis=1)
		        df_1["year"] = filename[0:4]
		        df_0 = df_0.append(df_1)
		    df_counter += 1

		if drop_flag == True:
			# reset the index and drop the duplicates
			df_0 = df_0.drop_duplicates(subset = ["CountrySource","year"])
		df_0.reset_index(drop=True)

	return df_0

def build_dict(dataframe):
	""" Get dictionary for words used
		Input:
		    dataframe with the information of the parquet files
		Output:
		    list with dictionary and what words and how many times they were used.
		    the words is still the code and not the words themselves
	"""	
	all_dicts = []
	# iterate through lists of words
	for out_list in dataframe["SpeechWordsList"]:
		# create a dicitonary for them
	    out_list = [s.strip(" ") for s in out_list]
	    dict_t = {}

	    # counter to know how many words were used
	    word_counter = 0
	    for emot_code in out_list:
	        if emot_code not in dict_t:
	            dict_t[emot_code] = 1
	        else:
	            dict_t[emot_code] += 1
	        word_counter += 1

	    # append to final list
	    all_dicts.append((word_counter,dict_t))

	return all_dicts

def dict_with_words(all_dicts,dataframe,HRE):
	""" Get dictionary with human readable words
		Input:
		    dataframe and dictionary with list of dictionaries
		Output:
		    the same list of dictionaries but with words instead of code for words
	"""

	for i in range(len(dataframe)):
	    dnumb = i
	    try:
	        del all_dicts[dnumb][1][""]
	    except:
	        pass
	    copy_dict = all_dicts[dnumb][1].copy()

	    for key in copy_dict.keys():
	        new_key = HRE.loc[HRE['Variable'] == key, "DimensionHumanName"].iloc[0]
	        all_dicts[dnumb][1][new_key] = all_dicts[dnumb][1][key]
	        del all_dicts[dnumb][1][key]

	return all_dicts



#################################################### Milestone 2 helper functions
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


def get_emotion(row):
	""" Get row referent to the event to see
	the emotions.
		Input:
		    row
		Output:
		    tuple list with dictionaryand number of words
	"""	
	dicts_word_count = []

	for cell in row.columns[1:]:
	    str_cell = row[cell].values[0]
	    
	    dicts_word_count.append((float(str_cell.split(':')[1]),str_cell.split(':')[0]))

	return dicts_word_count


def get_prevalent_emotions(event, df, Emotions_dictionary, top_features):
	""" Get the most frequent emotions for that even
		Input:
		    event, df, top_features
		Output:
		    Number of words for some emotion (not dictionary)
	"""	    
	event_emot = df.loc[df['GKGRECORDID'] == event]
	event_emot = event_emot.dropna(axis=1,how='all')

	d_wcnt = get_emotion(event_emot)

	d_wcnt.sort(reverse=True)

	word_count = d_wcnt[0][0]

	Numb_Emo_str = []

	for freq_emo in d_wcnt[1:top_features]:
	    
	    Emotion = Emotions_dictionary.loc[Emotions_dictionary['Variable'] == str(freq_emo[1])]
	    legend  = Emotion['DimensionHumanName'].values[0]
	    
	    Numb_Emo_str.append((freq_emo[0],legend))

	return Numb_Emo_str
    
def plot_commom_emotion(tuple_list = None, dictionary = None, dictionary_flag = False):
	""" Plot the most frequent emotions
		Input:
		    list
		Output:
		    plot
	"""	        
	if dictionary_flag == False:
	    x=[]
	    y=[]

	    for t in tuple_list:
	        x.append(t[0])
	        y.append(t[1])

	    plt.figure(figsize=(20,10))
	    plt.xticks(rotation=90)
	    plt.bar(y,x)
	    plt.show()

	else:
	    plt.figure(figsize=(20,10))
	    plt.xticks(rotation=90)
	    plt.bar(dictionary.keys(),dictionary.values())
	    plt.show()
    
def dict_top_feelings(df,Emotions_dictionary,start,end):
	""" Get dict of the most frequent emotions
		Input:
		    dataframe
		Output:
		    dictionary
	"""	 

	dict_t={}

	for i in df['GKGRECORDID'].iloc[start:end]:
	    emo = get_prevalent_emotions(i, df, Emotions_dictionary, 10 )
	    for feel in emo:
	        if feel[1] not in dict_t:
	            dict_t[feel[1]] = 1
	        else:
	            dict_t[feel[1]] += 1
	return dict_t