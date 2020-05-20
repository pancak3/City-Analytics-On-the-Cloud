const map = function (doc) {
    var dt = new Date(doc.created_at);
    var dayofweek = dt.toLocaleString('en-US', {timeZone: 'Australia/Melbourne', weekday: 'short'});
    emit(dayofweek, dayofweek);
}