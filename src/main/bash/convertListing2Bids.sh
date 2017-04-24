mvn compile exec:java \
      -Dexec.mainClass=org.davilj.trademe.dataflow.bids.ExtractValidBids \
      -Dexec.args="--project=tradememining \
      --stagingLocation=gs://trademedata/trademe_dataflow_staging/ \
      --output=gs://trademebids \
      --inputFile=gs://trademelistings/20170423/* \
      --errorFile=gs://trademebids_error \
      --runner=DataflowRunner"
