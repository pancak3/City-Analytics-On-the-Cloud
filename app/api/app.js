const path = requrie('path');
const express = require('express');
const cors = require('cors');
const proxy = require('express-http-proxy');
const app = express();

const db_username = process.env.DB_USERNAME;
const db_password = process.env.DB_PASSWORD;
const db_port = process.env.DB_PORT;

// For local development, bad practice for production use
app.use(cors());

// Serve static files
app.use(express.static(path.join(__dirname, 'client')));

// Root
app.get('/', (req, res) => {
    return res.send('COMP90024');
});

// Database status
app.get('/api', proxy(`http://${db_username}:${db_password}@couchdb:${db_port}`));

// Frontend
app.get('*', function(req, res) {
    return res.sendFile(path.join(__dirname, 'client/index.html'));
});

app.listen(process.env.PORT || 3000);
