# Same reasoning as deploy/prometheus/Dockerfile: provisioning + the committed dashboard JSON are
# baked in, so "provisioned, not hand-clicked" (WORKLOAD §2) is enforced by the image itself.
FROM grafana/grafana:11.3.1
COPY provisioning /etc/grafana/provisioning
COPY dashboards /etc/grafana/dashboards
