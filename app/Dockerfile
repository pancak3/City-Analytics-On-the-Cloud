# Build client and server
FROM node:14-alpine as builder

COPY ./api /build/api
RUN cd /build/api \
    && yarn install --production=false && yarn run build \
    && rm -rf node_modules \
    && yarn cache clean

COPY ./client /build/client
RUN cd /build/client \
    && yarn install --production && yarn run build \
    && rm -rf node_modules \
    && yarn cache clean

# Build server
FROM node:14-alpine as server

# Copy files
WORKDIR /app
COPY --from=builder /build/api/dist /app
COPY --from=builder /build/api/package.json /app
COPY --from=builder /build/client/build /app/client
ENV NODE_ENV production

# Install deps
RUN yarn install --production && yarn cache clean
RUN adduser -D comp90024
User comp90024

EXPOSE 3000
CMD [ "node", "app.js" ]
