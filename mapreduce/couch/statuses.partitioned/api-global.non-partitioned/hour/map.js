const map = function (doc) {
    var dt = new Date(doc.created_at);
    var dt_str = dt.toLocaleTimeString('en-US', {timeZone: 'Australia/Melbourne', hour12: false});
    var hour = Number(dt_str.split(':')[0]);
    emit(hour, hour);
}
