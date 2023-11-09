# Handling Data with PySpark

In this repo I showed how PySpark can be really useful while dealing with large datasets.
The Washington Post published a +100GB dataset with all the transactions regarding opioid consumption in the United States. The dataset is available [here](https://www.washingtonpost.com/national/2019/07/18/how-download-use-dea-pain-pills-database/?arc404=true).

First, I unzip the dataset directly from the url that contains the compressed file. Second I used PySpark to read the data and grouped by year, state and drug name. Finally, I saved the results in a csv file. To be imported in tableau for visualization. 