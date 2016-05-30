from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    print counts
    make_plot(counts)


def make_plot(counts):
	#positive word list
	p=[]
	#negative word list
	n=[]
	#range for axis
	range=0
	#generating list of positive and negative words
	for s in counts:
	   if s:
	      p.append(s[0][1])
              n.append(s[1][1])
	      range= max(s[0][1],s[0][1],range)
	# evenly sampled time at 200ms intervals
	q = np.arange(0., 12., 1)
	# creating x and y axis labels
	plt.xlabel('Time step') 
	plt.ylabel('Word Count')
	# setting axis format. +50 for fitting legends
	plt.axis([0, len(counts), 0, range +50])
	# blue circle line, green circle line
	plt.plot(q, p, 'bo-', label='positive')
	plt.plot(q, n, 'go-', label='negative')
	#creating legend with upper left position
	plt.legend(loc='upper left')
	plt.show()
    # YOUR CODE HERE


#Read from file and return each line as an element of list
def load_wordlist(filename):
	list = [line.rstrip('\n') for line in open(filename)]
	return list
    # YOUR CODE HERE
#Function for creating a stateless form of ReduceByKey operation
def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
	#creating a list for filtering positive || negative permissible words
    List = pwords + nwords 
	#changing key value to positive and negative for the RDD.
    counts = tweets.map(lambda line: line.split(" ")).flatMap(lambda line: line) \
        .map(lambda word: (word, 1)).filter(lambda x : x[0] in List).map(lambda x: ('positive', x[1]) if (x[0] in pwords) else ('negative', x[1]))
    # Add counts, tweets stores sum across all time stamps 
	tweets = counts.updateStateByKey(updateFunction)
	#DStream containing count only for this particular time range
    tweetsPerTime = counts.reduceByKey(lambda a, b: a+b)
    #positive = count.filter(lambda x : x[0] in nwords).map(lambda x: ('positive', x[1])) \
    #    .updateStateByKey(updateFunction)
    #negative = count.filter(lambda x : x[0] in nwords).map(lambda x: ('negative', x[1])) \
    #	.updateStateByKey(updateFunction)
    #tweets = positive.union(negative) 
    tweets.pprint()
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    tweetsPerTime.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()