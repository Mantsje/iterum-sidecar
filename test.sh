
export DATA_VOLUME_PATH="/tmp"
export ITERUM_NAME="sidecar"
export ITERUM_CONFIG='{"queue_mapping": {"outputA": "queueA", "outputB": "queueB"}}'
export PIPELINE_HASH="asdh37dHsf8H3fSSd24HEe35g2d4h754"
export MANAGER_URL="http://dummy-url"


export MQ_BROKER_URL="amqp://iterum:sinaasappel@localhost:5672"
#export MQ_INPUT_QUEUE="frag-out"
export MQ_INPUT_QUEUE="trans1-out"
#export MQ_OUTPUT_QUEUE="trans1-out"
export MQ_OUTPUT_QUEUE="trans2-out"

export MINIO_URL="localhost:9000"
export MINIO_ACCESS_KEY="iterum"
export MINIO_SECRET_KEY="banaanappel"
export MINIO_USE_SSL="false"
#export MINIO_OUTPUT_BUCKET="trans1-out"
export MINIO_OUTPUT_BUCKET="trans2-out"

export TRANSFORMATION_STEP_INPUT="tts.sock"
export TRANSFORMATION_STEP_OUTPUT="fts.sock"

make build
sidecar
