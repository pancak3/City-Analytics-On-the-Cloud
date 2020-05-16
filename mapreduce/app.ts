import _nano from 'nano';
// import checkViews from './couch/check-views';
import updateAreas from './couch/update-areas';

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

const init = async (): Promise<void> => {
    // Update areas
    await updateAreas(nano);
    // Check views
    // await checkViews(nano);
};

init().then(() => {
    console.log('init complete');
});
