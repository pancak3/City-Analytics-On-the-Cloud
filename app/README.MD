[//]: <> (@author Team 42, Melbourne, Steven Tang, 832031)

# API/Frontend
API is in `backend/` and frontend is in `frontend/`

During development, it's possible to start a development instance of the api locally (`npm devstart`) and work on the frontend (`yarn start`) with content served by the local instance of the backend.

However, the backend is designed to serve frontend pages in production.
The process of incorporating the frontend and backend are captured in the `Dockerfile`.

The .env file must be populated for local development to occur.
A sample file (`.env.sample`) is given.
