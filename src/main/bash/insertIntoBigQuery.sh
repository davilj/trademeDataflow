mvn compile exec:java \
      -Dexec.mainClass=org.davilj.trademe.dataflow.bids.Bids2BigQuery \
      -Dexec.args="--project=tradememining \
      --stagingLocation=gs://trademedata/trademe_dataflow_staging/ \
      --inputFile=gs://trademebids/20170423/* \
      --table=tradememining:trademeMining.sales_2017 \
      --tempLocation=gs://bgtemp \
      --runner=DataflowRunner"
