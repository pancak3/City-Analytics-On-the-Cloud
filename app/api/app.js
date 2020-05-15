const path = require('path');
const express = require('express');
const morgan = require('morgan');
const helmet = require('helmet');
const _nano = require('nano');
const app = express();

// Environment for local debug
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config();
}

// DB setup
const db_prot = process.env.DB_PROT || 'http';
const db_username = process.env.DB_USERNAME;
const db_password = process.env.DB_PASSWORD;
const db_port = process.env.DB_PORT || 5984;
const db_host = process.env.DB_HOST || 'couchdb';
const base_url = `${db_prot}://${db_username}:${db_password}@${db_host}:${db_port}`;
const nano = _nano(base_url);

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

// Database status
app.get('/api', async (req, res) => {
    const info = await nano.request({ path: '/' });
    return res.send(info);
});

// Gets and returns info about dbs
app.get('/api/dbs', async (req, res) => {
    const all_dbs_res = await nano.db.list();
    const db_info = [];
    for (const db of all_dbs_res) {
        const db_res = await nano.db.get(db);
        db_info.push(db_res);
    }
    return res.send(
        db_info.map((info) => {
            return {
                name: info.db_name,
                size: info.sizes.active,
                count: info.doc_count,
            };
        })
    );
});

// Gets users count with mapreduce
app.get('/api/users/count', async (req, res) => {
    const users = nano.db.use('users');
    const view = await users.view('api', 'count', { include_docs: false });
    return res.send(view);
});

// Frontend
app.get('*', function (req, res) {
    return res.sendFile(path.join(__dirname, 'client/index.html'));
});

// Listen
app.listen(process.env.PORT || 3000, () => {
    console.log(`[-] Listening on ${process.env.PORT || 3000}`);
});
