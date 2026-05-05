# aMule daemon — production container.
#
# Multistage Alpine build.  The runtime image carries amuled +
# (optionally) amulecmd and the minimal shared-library set, plus
# tini + su-exec for clean signal forwarding and uid switching.
#
# Build context: the repo root.
#   docker build -f packaging/docker/Dockerfile -t amule-fork:prod .
#
# Build args:
#   ALPINE_VERSION  — base Alpine for both stages (default 3.20)
#   ENABLE_UPNP     — 1 to link libupnp.  Defaults to 0 because the
#                     LAN deployment doesn't need miniupnp and dropping
#                     it shrinks the runtime image.
#   ENABLE_AMULECMD — 1 to also build amulecmd (CLI client).  Pulls
#                     readline-dev/readline at build/runtime.
#
# Default runtime command:
#   amuled -o    (foreground, log to stdout — Nomad-friendly)

ARG ALPINE_VERSION=3.20

# -----------------------------------------------------------------------------
# Stage 1: builder
# -----------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS builder

ARG ENABLE_UPNP=0
ARG ENABLE_AMULECMD=0

# Build deps.  cmake 3.10+ required by master's CMakeLists.
# wxwidgets-dev provides headless wxBase + wxNet headers + the wxGTK3
# variant, which the daemon links against (the daemon itself doesn't
# pull GTK at runtime — the linker only keeps referenced symbols).
RUN apk add --no-cache \
        build-base \
        cmake \
        ninja \
        pkgconfig \
        git \
        boost-dev \
        crypto++-dev \
        zlib-dev \
        wxwidgets-dev \
        gettext-dev \
        bsd-compat-headers \
        $( [ "$ENABLE_UPNP" = "1" ] && echo libupnp-dev ) \
        $( [ "$ENABLE_AMULECMD" = "1" ] && echo readline-dev )

WORKDIR /src
COPY . /src

# Out-of-source Release build.  GUI / remote GUI / webserver are
# off because they're useless in a headless container and dropping
# them shrinks the binary.
RUN cmake -S /src -B /build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_DAEMON=ON \
        -DBUILD_AMULECMD=${ENABLE_AMULECMD} \
        -DBUILD_MONOLITHIC=OFF \
        -DBUILD_REMOTEGUI=OFF \
        -DBUILD_WEBSERVER=OFF \
        -DENABLE_NLS=OFF \
        -DENABLE_UPNP=${ENABLE_UPNP} \
    && cmake --build /build --target amuled $( [ "$ENABLE_AMULECMD" = "1" ] && echo --target amulecmd ) \
    && strip /build/src/amuled \
    && if [ "$ENABLE_AMULECMD" = "1" ]; then \
           strip /build/src/amulecmd; \
           cp /build/src/amulecmd /build/_amulecmd_or_stub; \
       else \
           : > /build/_amulecmd_or_stub; \
       fi

# -----------------------------------------------------------------------------
# Stage 2: runtime
# -----------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

ARG ENABLE_UPNP=0
ARG ENABLE_AMULECMD=0

# Runtime libs.  wxwidgets-gtk3 is the wxBase + wxNet + wxGTK3
# runtime aMule actually uses (the GTK3 variant is the default
# packaging on Alpine; daemon doesn't open a display).
# crypto++ for ed2k crypto.  tini reaps zombie children and
# forwards signals; su-exec drops privileges from root → amule
# on entry.
RUN apk add --no-cache \
        wxwidgets-gtk3 \
        crypto++ \
        zlib \
        libgcc \
        libstdc++ \
        ca-certificates \
        tini \
        su-exec \
        $( [ "$ENABLE_UPNP" = "1" ] && echo libupnp ) \
        $( [ "$ENABLE_AMULECMD" = "1" ] && echo readline ) \
    && addgroup -S amule \
    && adduser -S -G amule -h /home/amule amule \
    && mkdir -p /incoming /temp /config /home/amule/.aMule \
    && chown -R amule:amule /incoming /temp /config /home/amule

COPY --from=builder /build/src/amuled /usr/local/bin/amuled
# amulecmd: COPY can't be conditional, so the builder stage always
# emits a stub at this path.  The runtime stage promotes it to the
# real binary or removes the stub.
COPY --from=builder /build/_amulecmd_or_stub /usr/local/bin/_amulecmd
RUN if [ -s /usr/local/bin/_amulecmd ]; then \
        mv /usr/local/bin/_amulecmd /usr/local/bin/amulecmd; \
    else \
        rm -f /usr/local/bin/_amulecmd; \
    fi

# ed2k TCP / Kad UDP / EC TCP / Prometheus /metrics.
EXPOSE 4662/tcp 4672/udp 4712/tcp 4713/tcp

VOLUME ["/incoming", "/temp", "/config", "/home/amule/.aMule"]

USER amule
WORKDIR /home/amule

# tini → amuled.  -o = foreground + log to stdout, the Nomad-friendly
# mode.  Override CMD to use amulecmd or another invocation.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["amuled", "-o"]
