# Build frontend and server
FROM node:14-alpine as builder

COPY ./backend /build/backend
RUN cd /build/backend \
    && yarn install --production=false \
    && yarn run build \
    && rm -rf node_modules \
    && yarn cache clean

COPY ./frontend /build/frontend
RUN cd /build/frontend \
    && yarn install --production \
    && REACT_APP_URL='' yarn run build \
    && rm -rf node_modules \
    && yarn cache clean

# Build server
FROM node:14-alpine as server

# Copy files
WORKDIR /app
COPY --from=builder /build/backend/dist /app
COPY --from=builder /build/backend/package.json /app
COPY --from=builder /build/frontend/build /app/frontend
ENV NODE_ENV production

# Install deps
RUN yarn install --production && yarn cache clean
RUN adduser -D comp90024
User comp90024

EXPOSE 3000
CMD [ "node", "app.js" ]
