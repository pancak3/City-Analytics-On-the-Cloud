import _nano from 'nano';
import express from 'express';
// import checkViews from './couch/check-views';
import updateAreas from './couch/update-areas';

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

// Serve a route indicating the commit that app is on
app.get('/', (req, res) => {
    res.send({ commit: process.env.COMMIT })
});

const init = async (): Promise<void> => {
    // Update areas
    await updateAreas(nano);
    // Check views
    // await checkViews(nano);
};

init().then(() => {
    console.log('init complete');

    app.listen(3000, () => {
        console.log('App listening on port 3000')
    });
});


