# Build client
FROM node:14-alpine as client
COPY ./client /build
ENV NODE_ENV production
RUN cd /build && yarn install --production && yarn run build

# Build server
FROM node:14-alpine

# Copy files
COPY ./api /app
COPY --from=client /build/build /app/client
WORKDIR /app

# Install deps
ENV NODE_ENV production
RUN yarn install --production
RUN adduser -D comp90024
User comp90024

EXPOSE 3000
CMD [ "node", "app.js" ]
