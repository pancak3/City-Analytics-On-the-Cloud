# Build app
FROM node:14-alpine as builder
COPY . /build
RUN cd build && yarn install --production=false && yarn run build  \
    && rm -rf node_modules/ && yarn cache clean

# Copy built app
FROM node:14-alpine
COPY --from=builder /build/dist /app
RUN cd /app && yarn install --production && yarn cache clean

RUN adduser -D comp90024
User comp90024
RUN ls -la /app/node_modules

EXPOSE 3000
CMD [ "node", "app.js" ]
