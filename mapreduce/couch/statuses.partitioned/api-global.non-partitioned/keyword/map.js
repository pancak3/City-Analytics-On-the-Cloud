const map = function (doc) {
    var sp_doc = doc.text.match(/\w+/g).map((s) => s.toLowerCase());
    var sp = [...new Set(sp_doc)];
    for (var i = 0; i < sp.length; i++) {
        emit([sp[i], doc.sa2_main16], 1);
    }
}