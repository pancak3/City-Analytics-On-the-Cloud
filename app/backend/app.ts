// @author Team 42, Melbourne, Steven Tang, 832031

import path from 'path';
import express, { NextFunction, Request, Response } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import _nano from 'nano';
import infoRouter from './info';
import scenarioRouter from './scenario';
const app = express();

// Environment variables for local debug
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config();
}

// Read env vars
const {
    DB_PROT = 'http',
    DB_USERNAME,
    DB_PORT = 5984,
    DB_PASSWORD,
    DB_HOST = 'couchdb',
} = process.env;
// DB setup
const nano = _nano(
    `${DB_PROT}://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}`
);

// Helmet
app.use(helmet());

// Spy on requests
app.use(morgan('combined'));

// For local development, bad practice for production use
if (process.env.NODE_ENV !== 'production') {
    const cors = require('cors');
    app.use(cors());
}

// Serve static files
app.use(express.static(path.join(__dirname, 'client')));

// Info routes
app.use('/api', infoRouter);
app.use('/api/scenarios', scenarioRouter);

// Frontend
app.get('*', function (req, res) {
    return res.sendFile(path.join(__dirname, 'client/index.html'));
});

// 404 handler
app.use((req, res) => {
    res.status(404);
    return res.json({ status: 'error', message: 'page not found' });
});

// Global error handler
app.use(function (err: Error, req: Request, res: Response, next: NextFunction) {
    res.status(500);
    return res.json({ status: 'error', message: err.message });
});

// Listen
app.listen(process.env.PORT || 3000, () => {
    console.log(`[-] Listening on ${process.env.PORT || 3000}`);
});

export default nano;
