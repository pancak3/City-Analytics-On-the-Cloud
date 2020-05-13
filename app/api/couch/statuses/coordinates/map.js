const map = function (doc) {
    if (doc.coordinates) {
        emit(doc._id, doc.coordinates);
    }
}