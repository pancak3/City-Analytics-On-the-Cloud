# Build client
FROM node:14-alpine as client
COPY ./client /client
ENV NODE_ENV production
RUN cd /client \
    && yarn install --production && yarn run build \
    && rm -rf node_modules

# Build server
FROM node:14-alpine

# Copy files
COPY ./api /app
COPY --from=client /client/build /app/client
WORKDIR /app

# Install deps
ENV NODE_ENV production
RUN yarn install --production
RUN adduser -D comp90024
User comp90024

EXPOSE 3000
CMD [ "node", "app.js" ]
