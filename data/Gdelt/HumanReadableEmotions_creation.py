import nltk
import pandas as pd
from nltk.corpus import words

DATA_LOCAL = "data/"

# read data extrated from the GCAM Master Codebook
data = pd.read_csv(DATA_LOCAL + "Emotions_dictionary",sep='\t')
data = data.drop(['Unnamed: 0'], axis=1)
data['DimensionHumanName'] = data['DimensionHumanName'].str.lower()

pos = pd.read_csv(DATA_LOCAL + "pos_words.csv", header = None)
pos = pos.rename(columns={0: "Words"})
pos_Words = list(pos["Words"])

Gen_words = open(DATA_LOCAL + "words_En.txt","r")
General_words = []
for line in Gen_words:
    General_words.append(line[:-1])

Neg_words = pd.read_csv(DATA_LOCAL + "neg_words.csv", header = None)
Neg_words = list(Neg_words[0])

P_N_Words = pd.read_csv(DATA_LOCAL + "P_N_words.csv", sep='\t', header = None)
P_N_Words = P_N_Words.drop(1, axis=1)
P_N_Words = list(P_N_Words[0])

# read it into a list so we can then work with pandas
count = 0
for word in data["DimensionHumanName"]:
    if count%500 == 0:
        print(count)
    if word in words.words() or word in pos_Words or word in P_N_Words or word in Neg_words or word in General_words:
        pass
    else:
        data.drop( data[ data['DimensionHumanName'] == word ].index , inplace=True)
    count+=1

# save data
data.to_csv("Human_Readable_Emotions_v2", sep='\t', encoding='utf-8')