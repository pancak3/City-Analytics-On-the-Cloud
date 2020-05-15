const checkViews = require('./couch/check-views');
const updateAreas = require('./couch/update-areas');
// Update areas
updateAreas();
// Check views (TODO: async)
checkViews();
