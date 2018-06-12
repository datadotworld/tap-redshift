FROM python:3.6-alpine

ADD . /app
WORKDIR /app

RUN apk update && \
 apk add postgresql-libs && \
 apk add --virtual .build-deps gcc musl-dev postgresql-dev && \
 pip install . && \
 apk --purge del .build-deps

CMD ["tap-redshift"]