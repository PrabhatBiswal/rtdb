# The console is NOT run as a container — this image is an ARTIFACT STORE, nothing more.
#
# Why it exists (WORKLOAD §5.13): the two console files are 124 KB, and every other way of getting
# them onto a replaced ops box is closed. EC2 `user_data` caps at 16 KB and `ops.sh` +
# `rtdb-ops-deploy.sh` already spend ~6 KB of it; SSM Parameter Store caps at 4 KB standard / 8 KB
# advanced. So the box has to FETCH them, and the only artifact store it already has credentials
# for and pulls from is ECR.
#
# `rtdb-ops-deploy.sh` does `docker create` / `docker cp` / `docker rm` — the container is never
# started, so nothing here ever executes. That matters: the ops instance sets
# `http_tokens = "required"` with no `http_put_response_hop_limit` (`main.tf:620-622`), so it
# defaults to 1, and the console must read SSM at RUNTIME to serve sign-in. Extracting the files and
# running them natively under `console.service` keeps the reviewed systemd hardening and keeps IMDS
# out of the question entirely.
#
# `scratch` because a base image would be 100 MB of runtime nothing will ever run.
FROM scratch
COPY auth-server.mjs /app/auth-server.mjs
COPY rtdb-console.html /app/rtdb-console.html
# NEVER EXECUTED, and required anyway. `docker create` refuses an image with no command at all —
# "Error response from daemon: no command specified" — even though nothing here is ever started and
# `scratch` has no interpreter to start it with. Measured: without this line the extraction in
# rtdb-ops-deploy.sh fails at `docker create`, which on a freshly replaced ops box means the console
# never installs. Naming the payload is the honest way to satisfy it, and it keeps the trick with
# the artifact instead of in every future caller.
CMD ["/app/auth-server.mjs"]
