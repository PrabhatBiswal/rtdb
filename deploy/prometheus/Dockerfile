# The config is BAKED IN rather than bind-mounted. Two reasons, one local and one that outlives it:
# the dev box's colima VM does not mount this repo (a bind mount there silently yields an empty
# directory, which looks like a running Prometheus scraping nothing), and an ops box that pulls a
# tagged image cannot drift from the committed file the way a host path can.
FROM prom/prometheus:v2.55.1
# Both configs ship in one image; the compose file picks with --config.file. Local rehearsal uses
# the static one, the ops box uses the ec2_sd one (Gate A ruling Q4).
COPY prometheus.yml /etc/prometheus/prometheus.yml
COPY prometheus.aws.yml /etc/prometheus/prometheus.aws.yml
