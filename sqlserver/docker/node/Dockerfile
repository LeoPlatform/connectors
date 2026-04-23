FROM node:10-alpine

WORKDIR /app/src

ARG UID
ARG GID
ENV GROUP=bot
ENV USER=bot

RUN apk add --no-cache vim python3 coreutils &&\
    if [ ! "$UID" -o ! "$GID" ]; then echo "Missing --build-args: UID and GID"; exit 1; fi; \
    cur_usr=$(getent passwd ${UID}|cut -d: -f1 2>/dev/null); \
    if [ $cur_usr ]; then deluser $cur_usr;  fi; \
    cur_grp=$(getent group ${GID}|cut -d: -f1 2>/dev/null) ; \
    if [ $cur_grp ]; then delgroup $cur_grp; fi;\
    addgroup -g $GID $GROUP &&\
    adduser -D -h /app -s /bin/bash -u $UID -G $GROUP $USER

RUN pip3 install --upgrade pip awscli &&\
    chown -R $UID:$GID /app &&\
    npm install -g npm@latest mocha standard leo-cli@2.5.2

# local shell requirements
RUN apk add --no-cache bash openssh curl tar \
      gcc libc-dev libffi-dev groff less \
      linux-headers make python3-dev

USER $USER

# docker build --no-cache --build-arg UID=1000 --build-arg GID=1000 -t data-bus-ingress-exigo:fullstack docker/fullstack/