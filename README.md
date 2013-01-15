# Storm Manual DRPC Demo

This is a demo of how to use DRPC to query the state of a Storm topology.

The topology consists of two streams:

* Default Stream:
 * Random Sentence Spout - emits random sentences
 * Word Splitter Bolt - splits sentence into words
 * Word Counter Bolt - counts occurrences of each word and saves count state in memory
* DRPC Stream
 * DRPC Spout - emits DRPC calls
 * Stream Change Bolt - ensures DRPC calls are emitted on a separate stream
 * Word Count Bolt - when DRPC call is received, it will emit word count state on DRPC stream
 * Return Bolt - returns results emitted on DRPC stream by Word Count bolt to DRPC

## Topology Design

The basic idea we demonstrate here, is how two streams can interconnect using DRPC.

Our default stream simply emits sentences and counts word occurrences. So far, nothing too complex.

Our DRPC stream emits calls from DRPCSpout via `StreamChangeBolt` to `WordCountBolt`, which in turn emits a response on the DRPC stream.

If you look in `RandomSentenceSpout`, `SplitSentenceBolt` and `WordCountBolt` you would note that the `declareOutputFields` 
method declares output on the default stream (i.e. calls `declarer.declare` rather than `declarer.declareStream`).

On the other hand, our `StreamChangeBolt` and `WordCountBolt` declare output on a named stream. In the case of `WordCountBolt`
it can emit tuples to both default stream and DRPC stream, hence there are two output stream declarations inside its `declareOutputFields`
method.

This is important because we want to ensure our DRPC calls are routed via a separate data stream to the default one. 
This separation of concerns helps us ensure that we can change our DRPC stream without worrying about the default stream.

## DRPC

If you read a bit about DRPC in Storm, you'll know that to ensure DRPC calls and responses are properly executed 
and returned, you need to emit an object containing the return information of the DRPC call (host, request ID etc.)

If you look inside `StreamChangeBolt` and `WordCountBolt` you'll see how this object is emitted from bolt to bolt 
and finally back to the DRPC `ReturnBolt`. When planning your DRPC calls, you have to make sure this object is passed around 
correctly, otherwise calls won't be completed.

Additionally, it's important to note that the actual call at the beginning of the DRPC chain is to the name of the DRPC Spout.

So, if your DRPC spout is called **drpc-query** DRPC calls must use **drpc-query** as their first argument (or DRPC function name).

Finally, when testing in local mode, you can pass a LocalDRPC instance to the DRPC Spout and execute calls on that instance, 
rather than starting up your own DRPC daemon.