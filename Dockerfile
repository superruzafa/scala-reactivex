FROM hseeberger/scala-sbt:8u171_2.12.6_1.1.6

VOLUME /code
VOLUME /root/.ivy2
VOLUME /root/.cache
VOLUME /root/.sbt
VOLUME /usr/share/sbt/lib

WORKDIR /code

ENTRYPOINT [ "/bin/bash" ]
