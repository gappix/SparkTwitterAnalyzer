# SPARK Twitter analyzer

We present a sample project that can get you started with the amazing **Apache SPARK**, a powerful and flexible Big Data processing tool. 
This application lets you perform both batch and streaming tweet elaboration.










## Getting Started









### Prerequisities 

In order to start testing this project you should have following objects:
* A linux-based performing Virtual Machine (at least 8GB RAM recommended) with:
	- Hadoop and Spark installed on (we suggest the **Hortonworks HDP Sandbox**: http://hortonworks.com/products/sandbox/)
	- latest version of **Java** installed
	- Internet access
* A Scala-friendly IDE like: http://scala-ide.org/





### Installing 		

For a correct HDP Sandbox install and in order to getting started with **Ambari** user and administration interfaces, just follow the official tutorial instructions: http://hortonworks.com/hadoop-tutorial/learning-the-ropes-of-the-hortonworks-sandbox/ 
Once you feel confident with your Hadoop environment, just clone this project into a local workspace directory and you'll be ready to start!


NOTE that this application is fully written in **Scala**, which is a functional Java super-set language.
*If you're already familiar with Java it should be quite understandable at first glance. Anyway, there is plenty of resources on the web (like: http://www.tutorialspoint.com/scala/scala_quick_guide.htm ) may help you taking quickly the edge of the learning curve.*



## Running the app
In order to launch the app execution, source code must be packed and exported into a **jar** file.
Then it must be transferred to the virtual machine in a directory of your choice (FileZilla software could come in handy for that).
According to desired functionality there are different additional sources which have to come with the jar package.




### Batch processing	



####General description
In *batch mode* the SPARK Twitter Analyzer extracts information from a Json-row database source file which contains tweets data. It extracts interesting informations packing them into a DataFrame structure. Sentiment evaluation is then obtained by joining tweets with an "Hedonometer dictionary" which assign to each word an happiness value. All values are then averaged obtaining an approximate sentiment value for each tweet message.
Everything is eventually stored as table into Hive, an Hadoop SQL-like datawarehouse infrastructure integrated in your Hortonworks distribution.




####What you need
From the Ambari interface, you must create a **Tutorials/SPARKTwitterAnalyzer** folder into your HDFS */user/maria_dev* path (if maria_dev folder doesn't already exists then create it too).
Into your just-created HDFS */user/maria_dev/Tutorials/SPARKTwitterAnalyzer* folder you must load following files:
* **HedonometerNOHEADER.txt**
* **RawTweets.json**

which can be found in project **resources** source folder


####Execution
In your virtual machine folder where app.jar file is located launch everything with following command: 
```
# spark-submit --class it.reti.spark.sentiment.Main APP_FILENAME.jar  batch
```
Elaboration results samples are eventually printed in a table-like format. 
You can access to stored informations using Ambari's HIVE View, or connecting any program (like  Qlik) to your Hortonworks Platform via ODBC.




### Streaming processing	

####General description
Streaming processing mode executes the same *batch* processing steps to every bunch of tweets coming from a Twitter Stream Spout every 25 seconds. It gives an idea of SPARK flexibility power which let you adapt your already-written  batch processing code for a streaming usage.



####What you need
Like *batch* case, you must have created (using the Ambari interface) a */user/maria_dev/Tutorials/SPARKTwitterAnalyzer* folder where to load following files:
* **HedonometerNOHEADER.txt**


Moreover you must now add, in the same Virtual Machine folder where the app **jar** package is located, following **jar dependencies**:
* **spark-streaming-twitter_2.10-1.6.0.jar**
 http://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter_2.10/1.6.0
* **twitter4j-core-4.0.4.jar**
  http://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
* **twitter4j-stream-4.0.4.jar**
  http://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream/4.0.4




####Execution
In your virtual machine folder where app.jar file and dependencies are located launch: 
```
# spark-submit  --class it.reti.spark.sentiment.Main  --jars spark-streaming-twitter_2.10-1.6.0.jar,twitter4j-core-4.0.4.jar,twitter4j-stream-4.0.4.jar APP_FILENAME.jar  streaming
```
You'll be asked to select a region to spy form a pre-defined list.
After the environment initialization you should start see incoming tweets and sentiment elaboration results printed in tables every 25 seconds interval.










## Built With

* Maven 













## Authors

* Andrea Biancini
* Paolo Gazzotti








