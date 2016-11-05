mvn compile exec:java \
      -Dexec.mainClass=org.davilj.trademe.dataflow.reports.DailySales \
      -Dexec.args="--project=tradememining \
      --stagingLocation=gs://trademedata/staging/ \
      --output=gs://trademedata/reports/dailySales/ds_201512.txt \
      --inputFile=gs://trademedata/results/d_201512.txt \
      --runner=BlockingDataflowPipelineRunner"
