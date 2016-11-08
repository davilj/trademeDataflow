mvn compile exec:java \
      -Dexec.mainClass=org.davilj.trademe.dataflow.reports.DailySales \
      -Dexec.args="--project=tradememining \
      --stagingLocation=gs://trademedata/staging/ \
      --inputFile=gs://trademedata/results/d_201512.txt \
      --errorFile=gs://trademedata/reports/dailySales/ds_201512.error.txt \
      --output=gs://trademedata/reports/dailySales/ds_201512.txt \
      --runner=BlockingDataflowPipelineRunner"
      
      
      
