mvn compile exec:java \
      -Dexec.mainClass=org.davilj.trademe.dataflow.formatters.ParseDataFiles \
      -Dexec.args="--project=tradememining \
      --stagingLocation=gs://trademedata/staging/ \
      --output=gs://trademedata/d_201611 \
      --inputFile=gs://trademedata/d_201611.merge.zip \
      --runner=BlockingDataflowPipelineRunner"
