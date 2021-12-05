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