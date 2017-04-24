mvn compile exec:java \
      -Dexec.mainClass=org.davilj.trademe.dataflow.listings.ParseDataFiles \
      -Dexec.args="--project=tradememining \
      --stagingLocation=gs://trademedata/trademe_dataflow_staging/ \
      --output=gs://trademelistings \
      --inputFile=gs://tradmerawdata/20170423.zip \
      --runner=DataflowRunner"
