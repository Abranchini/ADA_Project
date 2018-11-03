# LET'S GET EMOTIONAL (we already got physical)

## Abstract

In our data story we explore conflicts all over the world and the public attention they receive. We harness the huge [GDELT 2.0](https://www.gdeltproject.org/data.html#documentation) dataset consisting of events collected every day from thousands media channels of the world's news. The GDELT contains various features about the particular events including the sentiment analysis of the news sources. We focus mostly on the emotions conflicts tend to arouse with respect to their distinct features. The questions we ask are whether some regions are payed more attention than others, if news articles get more emotionally involved in conflicts occuring closer to our homes, or whether we tend to depict particular regions, nations or ethnicities by some steady emotional patterns. Throughout our research we hope to lift the shroud of how equally media report on the worlds conflicts and whether some sentiment bias exists. As the public is nowadays heavily influenced by (social) media and emotions affect us often heavier than the pure facts, we believe it is an important task to investigate how our news sources work with such a power they have upon us. 


## Research question

- **Are we emotionally biased?** Do the number of conflicts or their distance from our home define our emotions? Is there an underlying trend of a more positive or negative news perception over time?

- **Are some countries ignored in the news?**  Is the number of conflicts taking place in a country in relation with the number of mentions in the media depending on where the conflict has happened? 

- **Are we emotionally predictable?** Can we observe patterns of emotions with respect to a country, religion or an ethnical group? Can we derive a model predicting emotions in case of a new conflict based on its specific features?

- **Do we have a saturation limit?** Does increasing number of conflicts make people feel worse and worse or is there some limit? Do we get used to a conflict with time and become less sentimental? 


**---TO BE DEFINED (as a 5th, 6th question) ---**
- Research question correlating the average tone with a specific event/ ethnical group/mfamous person/ dictator 

- In line with: are we emotionally biased? If we select some countries important in the global political spectrum (e.g. Germany, Russia, Brazil and US) and we look at the timeline (e.g. 1990 until today) of the average tone of each country in articles mentionning: "country" "President (at that time period)" "Name" (e.g. "Germany " "chancellor" "Angela Merkel" if we are in the time period 2005-2018 and "Germany" "chancellor" "Gerhard Schröder" 1998-2005)

- Do we see generally differences between the countries (e.g. more negative for Russia than Germany) Do we see pattern when the president changes? Do we see a trend towards more negative emotions over the years?


## Dataset
As mentioned in the abstract the dataset that we will use to answer our questions is the GDELT dataset. It has five main datafields ( EventID and Date , Actor, Envent ,Event Geography and Data Management Fields), with each one having several attributes. We plan to focus on the location of the events or the actors, the goldstein scale (or a scale proposed by us) and the AvgTone (plus some realtime measurement of 2,300 Emotions) to build the answer to our questions. The sourceurl attribute will also help us to determine reactions of countries to a certain event.   
We will make an analysis on a country , religion and ethnical group basis as to find possible results in terms of emotions regarding the events, narrowing our search in the dataset. Since GDELT 2.0 is up to date, some of the main conflicts should be well described in the web, something we can use to enrich our analysis regarding the reactions of some countries to that event.

## A list of internal milestones (until milestone 2)
Until the 10th of November (Data Acquisition , Data Preparation and Cleaning)
- Get to know how to work in the cluster.
- Exploratory analysis of the GDELT dataset.
- Divide the dataset taking into account some specific countries/ethnic and religious groups desired for our research.
- Clean the dataset regarding possible events that are not interesting.
- Sketch our own goldstein scale for future comparison.

Until the 17th of November (Data Preparation and Cleaning, Data interpretation)
- Search the urls from the news of chosen events to be able to attribute them to countries.
- Having the news and the countries/ethnic and religious groups defined begin our emotional analysis. Observe how they react.
- Select specific timeframes where we can see a possible emotion saturation

Until the 25th of November (Data interpretation) 
- From the patterns discovered before, build a possible predictive model that would enables to antecipate possible emotional reactions to a new conflict.
- Enrich possible big event conclusions with web data.


## Questions for TAs

- How old is the GDELT dataset in the cluster? How regularly is it updated?
- Do we have access to the 2300 emotions provided for each event or just the overall sentiment? GDELT mentions that: "users interested in emotional measures use the Mentions and Global Knowledge Graph tables to merge the complete set of 2,300 emotions and themes from the GKG GCAM system into their analysis of event records"
