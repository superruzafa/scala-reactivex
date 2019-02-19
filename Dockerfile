FROM hseeberger/scala-sbt

VOLUME /code
VOLUME /root/.ivy2
VOLUME /root/.cache
VOLUME /root/.sbt
VOLUME /usr/share/sbt/lib

WORKDIR /code

ENTRYPOINT [ "/bin/bash" ]
