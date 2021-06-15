FROM mhart/alpine-node:slim-16.2.0

COPY build/index.cjs /index.cjs

CMD ["node", "/index.cjs"]
