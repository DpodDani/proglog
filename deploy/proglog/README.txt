The Chart.yaml file describes your chart.
You can access the data in this file in your templates.
Note: the charts directory contains sub-charts!

The values.yaml file contains the chart's default values.
Users have the ability to override these values when they install/upgrade your
chart.

The templates directory contains template files that you render with your
values to generate valid Kubernetes manifest files. Kubernetes applies rendered
manifest files to install resources needed for service. We will write Helm
templates using the Go template language.

Templates can be rendered locally without applying resources in Kubernetes
cluster. Allows you to see the rendered resources that Kubernetes will apply.

Command for local rendering: $ helm template <chart_directory>

===============================================================================

We will use two Helm resource types:
1) StatefulSets --> manage stateful applications in Kubernetes (eg. our service
that persists a log)
    Used when a service requires one or more of the following:
    * Stable, unique network identifiers - each node in our service requires
    unique node names as identifiers
    * Stable, persistent storage - our service expects the data its written to
    persist across restarts
    * Ordered, graceful deployment and scaling - our service needs initial code
    to bootstrap the cluster and join subsequent nodes to its cluster
    * Ordered, automated rolling updates - we always want our cluster to have a
    leader, and when we roll the leader we want to give the cluster enough time
    to elect a new leader before rolling the next node

    "Stable" in this context means persistence across scheduled changes like
    restarts and scaling.

    If a service is not stateful (eg. an API service that persists data to
    Postgres) then a Deployment should be used instead of StatefulSet!

2) Service
    TBC

===============================================================================

Probes --> Kubernetes uses probes to know whether it needs to act on container
to improve service's reliability. Usually the probe requests a health check
endpoint that responds with the health of the service.

Three types of probes:
1) Liveness probes --> signal that container is alive, otherwise Kubernetes
will restart the container. Kubernetes calls the liveness probe through the
container's lifetime.

2) Readiness probes --> check that container is ready to accept traffic,
otherwise Kubernetes will remove the pod from the service load balancers.
Kubernetes calls the readiness probe through the container's lifetime.

3) Startup probes --> signal when container application has started and
Kubernetes can begin probing for liveness and readiness. Distributed services
often need to go through service discovery and join in consensus with cluster
before they're initialised.
If we had liveness probe that failed before service
has finished initialising, our service would continually restart.
Afer startup, Kubernetes doesn't call this probe again.

These probes improve a service's reliability, but can also cause incidents if
not configured correctly.

Three ways of running probes:
1) Making HTTP request against a server
2) Opening TCP socket against socket
3) Running command in container (eg. the pg_isready command for Postgres DB)

First two are lightweight and don't require extra binaries in your image.
However, a command can be more precise and necessary if you use your own
protocol.

gRPC conventionally uses a grpc_health_probe command that expects your
server to satisfy gRPC health checking protocol.

===============================================================================

Kubernetes services --> exposes an application as a network service.
You define a Service with policies that specify what Pods the Service applies
to and how to access the Pods.

Four types of services specify how the Service exposes the Pods:
1) ClusterIP --> exposes the Service on a load-balanced cluster-internal IP so
the Service is reachable within Kubernetes cluster only <-- this is the default
Service type.

2) NodePort --> exposes Service on each Node's IP on static port - even if Node
doesn't have Pod on it. Kubernetes still sets up routing so if you request a
Node at the service's port, it'll direct request to the proper place.
You can request NodePort services outside the Kubernetes cluster!
(Remember, a Node is a physical host, whereas a Pod is a logical host - so
multiple Pods can run on a single Node!)

3) LoadBalancer --> exposes the Service externally using cloud provider's load
balancer. This type of Service automatically creates ClusterIP and NodeIP
services behind the scenes, and manages the routes to these services.

4) ExternalName --> special Service that serves as way to alias a DNS name.

Travis doesn't recommend using NodePort services (directly) because you then
have to know your nodes' IPs to use the services, must secure all your Nodes,
and have to deal with port conflicts.
Instead, he recommends using a LoadBalancer/ClusterIP service if you're able
to run a Pod that can access your internal network.