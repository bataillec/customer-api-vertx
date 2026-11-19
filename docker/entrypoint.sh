#!/bin/sh
# The event bus advertises the host it is given to the other nodes: the default (localhost) would
# make every node dial itself, so it has to be this container's address on the compose network.
CLUSTER_HOST=$(hostname -i | cut -d' ' -f1)

exec java \
  -Dvertx.hazelcast.config=/app/cluster.xml \
  -cp /app/app.jar \
  io.vertx.launcher.application.VertxApplication "$@" \
  -c --cluster-host "$CLUSTER_HOST"
