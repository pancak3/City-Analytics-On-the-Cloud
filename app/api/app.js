const path = requrie('path');
const express = require('express');
const cors = require('cors');
const proxy = require('express-http-proxy');
const app = express();

const username = process.env.DB_USERNAME;
const password = process.env.DB_PASSWORD;

// For local development, bad practice for production use
app.use(cors());

// Serve static files
app.use(express.static(path.join(__dirname, 'client')));

// Root
app.get('/', (req, res) => {
    return res.send('COMP90024');
});

// Database status
app.get('/api', proxy(`http://${username}:${password}@couchdb`));

// Frontend
app.get('*', function(req, res) {
    return res.sendFile(path.join(__dirname, 'client/index.html'));
});

app.listen(process.env.PORT || 3000);
