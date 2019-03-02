# OpenCensus Agent/Collector Demo

Typical flow of tracing data with OpenCensus service: tracing data initially received by OC Agent
and then sent OC Collector using OC data format. The OC Collector then sends the data to the
tracing backend, in this demo Jaeger and Zipkin.

This demo uses `docker-compose` and runs against docker images of OC service.
To run the demo, switch to the `demos/trace` folder and run:

```shell
docker-compose up -d
```

Open `http://localhost:16686` to see the data on the Jaeger backend and `http://localhost:9411` to see
the data on the Zipkin backend.

To clean up any docker container from the demo run `docker-compose down` from the `demos/trace` folder.
