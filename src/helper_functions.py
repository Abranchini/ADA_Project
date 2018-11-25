import pandas as pd
import matplotlib.pyplot as plt

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