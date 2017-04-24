#this does not work after migration to apache beam

cd ../../..
pwd


mvn compile exec:java \
 -Dexec.mainClass=org.davilj.trademe.dataflow.listings.ParseDataFiles \
 -Dexec.args="--project=tradememining \
              --stagingLocation=gs://trademe_dataflow_staging/listings \
              --dataflowJobFile=gs://trademe_dataflow_staging/templates/ParseDataFiles \
              --runner=TemplatingDataflowPipelineRunner"
