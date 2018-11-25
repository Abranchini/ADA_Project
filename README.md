# LET'S GET EMOTIONAL (we already got physical)

## Abstract

In our data story we explore conflicts all over the world and the public attention they receive. We harness the huge [GDELT 2.0](https://www.gdeltproject.org/data.html#documentation) dataset consisting of events collected every day from thousands media channels of the world's news. The GDELT contains various features about the particular events including the sentiment analysis of the news sources. We focus mostly on the emotions conflicts tend to arouse with respect to their distinct features. The questions we ask are whether some regions are payed more attention than others, if news articles get more emotionally involved in conflicts occuring closer to our homes, or whether we tend to depict particular regions, nations or ethnicities by some steady emotional patterns. Throughout our research we hope to lift the shroud of how equally media report on the worlds conflicts and whether some sentiment bias exists. As the public is nowadays heavily influenced by (social) media and emotions affect us often heavier than the pure facts, we believe it is an important task to investigate how our news sources work with such a power they have upon us. 


---


## Research question

- **Are we emotionally biased?** Do the number of conflicts or their distance from our home define our emotions? Is there an underlying trend of a more positive or negative news perception over time?

**Update:** We do not evaluate patterns over time since the available dataset only covers a period of 2 years. 

- **Are some countries ignored in the news?**  Is the number of conflicts taking place in a country in relation with the number of mentions in the media depending on where the conflict has happened? 

- **Are we emotionally predictable?** Can we observe patterns of emotions with respect to a country, religion or an ethnical group? Can we derive a model predicting emotions in case of a new conflict based on its specific features?

- **Do we have a saturation limit?** Does increasing number of conflicts make people feel worse and worse or is there some limit? Do we get used to a conflict with time and become less sentimental? 

- **Who is more emotional?** Do we see sensitivity differences between some countries? Do we see a trend towards more negative emotions over the years?

**Update:** We will focus also on actors and not countries because the emotions are referent to the news, so it makes more sense to associate certain types of speech to people rather than countries (although there can also be some interesting relationship here) 

## Dataset
As mentioned in the abstract the dataset that we will use to answer our questions is the GDELT dataset. It has five main datafields ( EventID and Date , Actor, Envent ,Event Geography and Data Management Fields), with each one having several attributes. We plan to focus on the location of the events or the actors, the goldstein scale (or a scale proposed by us) and the AvgTone (plus some realtime measurement of 2,300 Emotions) to build the answer to our questions. The sourceurl attribute will also help us to determine reactions of countries to a certain event.   
We will make an analysis on a country , religion and ethnical group basis as to find possible results in terms of emotions regarding the events, narrowing our search in the dataset. Since GDELT 2.0 is up to date, some of the main conflicts should be well described in the web, something we can use to enrich our analysis regarding the reactions of some countries to that event. Moreover, our second question (Are some countries ignored in the news?), will benefit from the new "High Resolution View of the Non-Western World" capability added in GDELT 2.0, that expectantly will provide us with more accurate and unbiased data regarding more remote countries.


---


## A list of internal milestones (until milestone 2)
Until the 10th of November (Data Acquisition , Data Preparation and Cleaning)
- Get to know how to work in the cluster.
- Exploratory analysis of the GDELT dataset.
- Create subsets of the dataset taking into account some specific countries/ethnic and religious groups desired for our research.
- Clean the dataset regarding possible events that are not interesting.
- Sketch our own goldstein scale for future comparison.

Until the 17th of November (Data Preparation and Cleaning, Data interpretation)
- Search the urls from the news of chosen events to be able to attribute them to countries.
- Having the news and the countries/ethnic and religious groups defined, begin our emotional analysis. Observe how they react.
- Select specific timeframes where we can see a possible emotion saturation
- Relate importance of conflict with attention it got by other countries.

Until the 25th of November (Data interpretation) 
- From the patterns discovered before, build a possible predictive model that would enable to antecipate possible emotional reactions to a new conflict.
- Enrich our potential conclusions regarding reactions to big events with web data.


## A list of internal milestones (until milestone 3)

- Think about different visualizations to present the conducted data analysis in a clear and concise manner.
- Develop the model for question 3 (that will take into account a lot of the results of the other questions).
- Explore the different emotion metrics, average tone and emotion dictionaries and propose a propose a feasible combination of all of them. Also define a score referent to the dictionaries in the GCAM column (similar to the vscore GCAM already has).
- We have run and tested pieces of our code on the cluster already on a couple of files. Next step is to run our whole pipeline on the whole dataset in the cluster.
- Create a template for our data story and fill the individual sections.
- Answer questions we have asked ourselves in the first milestone with appropriate data reasoning.


---


## Questions for TAs - Milestone 1

- How old is the GDELT dataset in the cluster? How regularly is it updated?
- Do we have access to the 2300 emotions provided for each event or just the overall sentiment? GDELT mentions that: "users interested in emotional measures use the Mentions and Global Knowledge Graph tables to merge the complete set of 2,300 emotions and themes from the GKG GCAM system into their analysis of event records"


## Questions for TAs - Milestone 2

- In the GCAM column, the floating point average value (the ones that start with v) do not exist for all dictionaries in that event.
There are a lot of c's, but not a corresponding number of v's. In the documentation this fact is not explained, wanted to know if they only computed the score for specific dictionaries? Also, where could we find some definitions of the "feeling" of the dictionaries? Some are stated in the Codebook, others have excel sheets in the web, but some don't, if you knew where the GDELT goes to obtain this information would be great.
