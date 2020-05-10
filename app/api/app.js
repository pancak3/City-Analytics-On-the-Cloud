const path = require('path');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const morgan = require('morgan');
const helmet = require('helmet');
const app = express();

// DB setup
const db_username = process.env.DB_USERNAME;
const db_password = process.env.DB_PASSWORD;
const db_port = process.env.DB_PORT || 5984;
const db_host = process.env.DB_HOST || "couchdb";
const base_url = `http://${db_username}:${db_password}@${db_host}:${db_port}`;

// Helmet
app.use(helmet());

// Spy on requests
app.use(morgan('combined'))

// For local development, bad practice for production use
app.use(cors());

// Serve static files
app.use(express.static(path.join(__dirname, 'client')));

// Database status
app.get('/api', async (req, res) => {
    const resp = await axios.get(base_url, { responseType: 'stream' });
    resp.data.pipe(res);
});

// Frontend
app.get('*', function(req, res) {
    return res.sendFile(path.join(__dirname, 'client/index.html'));
});

app.listen(process.env.PORT || 3000);
